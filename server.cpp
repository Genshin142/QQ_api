#include "server.h"
#include "ui_server.h"
#include <QDebug>
#include <QDateTime>
#include <QFile>
#include <QTextStream>
#include<QJsonDocument>
#include<QJsonObject>
#include <QCryptographicHash>  // 用于密码加密
#include<QSqlError>
#include <QJsonArray>
#include <QDir>
#include <QUrlQuery>

// 数据库配置已移至 ConnectionPool

Widget::Widget(QWidget *parent)
    : QWidget(parent)
    , ui(new Ui::Widget)
{
    ui->setupUi(this);
    server = new QTcpServer(this);

    m_threadPool = new QThreadPool(this);
    m_threadPool->setMaxThreadCount(QThread::idealThreadCount()); // 根据核心数设置线程数
    m_threadPool->setExpiryTimeout(-1); // 禁用空闲线程销毁，稳定数据库连接

    if (!server->listen(QHostAddress::Any, 8080)) {
        return;
    }

    connect(server, &QTcpServer::newConnection, this, &Widget::handleIncomingConnection);// 监听新连接
}

Widget::~Widget() {
    if (server->isListening()) {
        server->close();
    }
    ConnectionPool::release();
    delete ui;
}

void Widget::handleIncomingConnection() {
    while (QTcpSocket* socket = server->nextPendingConnection()) {
        m_pendingSockets.insert(socket);
        connect(socket, &QTcpSocket::readyRead, this, [this, socket]() {
            handleProtocolDetection(socket);
        });
        connect(socket, &QTcpSocket::disconnected, this, [this, socket]() {
            m_pendingSockets.remove(socket);
            m_tcpBuffers.remove(socket); // 清理缓冲区
            
            // 确保从在线列表中移除
            QMutexLocker locker(&m_onlineClientsMutex);
            for (auto it = m_onlineClients.begin(); it != m_onlineClients.end(); ++it) {
                if (it.value() == socket) {
                    m_onlineClients.erase(it);
                    break;
                }
            }
            socket->deleteLater();
        });
    }
}

void Widget::handleProtocolDetection(QTcpSocket *socket) {
    // 统一改为直接处理数据，不再进行 HTTP/WS 探测
    disconnect(socket, &QTcpSocket::readyRead, nullptr, nullptr);
    m_pendingSockets.remove(socket);
    connect(socket, &QTcpSocket::readyRead, this, [this, socket]() {
        handleData(socket);
    });
    handleData(socket); // 立即处理第一批数据
}

// 移除原有的 WebSocket 槽函数实现


// 移除 sendWsPacket，统一使用 sendPacket

// 实现发送消息处理函数
void Widget::handleSendMessage(const QJsonObject& requestData, QTcpSocket* socket)
{
    // 提取消息参数
    QString fromUserId = requestData["fromUserId"].toString().trimmed();
    QString toUserId = requestData["toUserId"].toString().trimmed();
    QString content = requestData["content"].toString();

    // 验证必要参数
    if (fromUserId.isEmpty() || toUserId.isEmpty() || content.isEmpty()) {
        QJsonObject response;
        response["type"] = "send_message_res";
        response["success"] = false;
        response["error"] = "缺少必要参数";
        sendPacket(socket, response);
        return;
    }

    // 保存消息到数据库，获取消息ID
    QString msgId = saveMessage(fromUserId, toUserId, content);
    bool success = !msgId.isEmpty();

    // 构建对发送方的响应
    QJsonObject response;
    response["type"] = "send_message_res";
    if (success) {
        response["success"] = true;
        response["id"] = msgId;
        response["toUserId"] = toUserId;
        response["content"] = content;
        sendPacket(socket, response);

        // --- 核心：实时推送逻辑 ---
        QJsonObject pushMsg;
        pushMsg["type"] = "new_message";
        pushMsg["id"] = msgId; // 新增：全局唯一消息ID
        pushMsg["fromUserId"] = fromUserId;
        pushMsg["toUserId"] = toUserId; 
        pushMsg["content"] = content;
        pushMsg["time"] = QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss");

        if (toUserId == "0000") {
            // 群聊：推送到除发送者外的所有在线客户端
            pushMsg["isGroup"] = true;
            QMutexLocker locker(&m_onlineClientsMutex);
            for (auto targetSocket : m_onlineClients.values()) {
                if (targetSocket != socket) {
                    sendPacket(targetSocket, pushMsg);
                }
            }
        } else {
            QMutexLocker locker(&m_onlineClientsMutex);
            if (m_onlineClients.contains(toUserId)) {
                // 单聊：若接收方在线，直接推送
                sendPacket(m_onlineClients[toUserId], pushMsg);
            }
        }
    } else {
        response["success"] = false;
        response["error"] = "消息发送失败";
        sendPacket(socket, response);
    }
}

// 实现消息保存到数据库的函数，返回插入的消息ID（失败返回""）
QString Widget::saveMessage(const QString& fromUserId, const QString& toUserId, const QString& content)
{
    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen()) return "";

    QSqlQuery query(db);
    query.prepare("INSERT INTO messages (from_user_id, to_user_id, content, send_time) "
                  "VALUES (:from, :to, :content, NOW())");
    query.bindValue(":from", fromUserId);
    query.bindValue(":to", toUserId);
    query.bindValue(":content", content);

    if (query.exec()) {
        QString id = query.lastInsertId().toString();
        ConnectionPool::closeConnection(db);
        return id;
    }
    ConnectionPool::closeConnection(db);
    return "";
}

QList<QJsonObject> Widget::queryMessages(const QString &m_myUserId, const QString &toUserId, const QString &m_lastMessageId)
{
    QList<QJsonObject> messages;
    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen()) return messages;

    QSqlQuery query(db);
    // ... rest of logic ...
    QString sql;

    // 群聊查询逻辑（群聊ID为"0000"）
    if (toUserId == "0000") {
        // 群聊消息：查询所有发送到群聊的消息，不限制发送者
        sql = "SELECT id, from_user_id, content, send_time FROM messages "
              "WHERE to_user_id = :toUserId "  // 仅按群聊ID过滤
              "AND id > :m_lastMessageId "
              "ORDER BY send_time ASC";
    } else {
        // 单聊消息：双向查询
        sql = "SELECT id, from_user_id, content, send_time FROM messages "
              "WHERE ((from_user_id = :m_myUserId AND to_user_id = :toUserId) "
              "OR (from_user_id = :toUserId AND to_user_id = :m_myUserId)) "
              "AND id > :m_lastMessageId "
              "ORDER BY send_time ASC";
    }

    query.prepare(sql);
    query.bindValue(":m_myUserId", m_myUserId);
    query.bindValue(":toUserId", toUserId);
    query.bindValue(":m_lastMessageId", m_lastMessageId);

    if (!query.exec()) {
        return messages;
    }

    // 遍历查询结果并转换为JSON对象
    while (query.next()) {
        QJsonObject msg;
        msg["id"] = query.value(0).toString();  // 消息ID
        msg["fromUserId"] = query.value(1).toString();  // 发送者ID
        msg["content"] = query.value(2).toString();     // 消息内容
        msg["time"] = query.value(3).toDateTime().toString("yyyy-MM-dd hh:mm:ss");  // 发送时间
        messages.append(msg);
    }

    ConnectionPool::closeConnection(db);
    return messages;
}
// 处理 TCP 数据流：实现拆包（分帧）
void Widget::handleData(QTcpSocket *socket)
{
    // 获取/创建属于该 socket 的缓冲区
    QByteArray &buffer = m_tcpBuffers[socket];
    buffer.append(socket->readAll());

    while (buffer.size() >= 4) {
        // 读取 4 字节大端整数作为数据包长度
        QDataStream ds(buffer);
        quint32 packetSize;
        ds >> packetSize;

        if (buffer.size() < 4 + packetSize) {
            // 数据包不完整，等待更多数据
            break;
        }

        // 提取 JSON内容
        QByteArray jsonData = buffer.mid(4, packetSize);
        buffer.remove(0, 4 + packetSize);

        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (!doc.isNull() && doc.isObject()) {
            QJsonObject json = doc.object();
            // 将业务逻辑提交到线程池执行（QThreadPool 内部维护任务队列）
            m_threadPool->start([this, socket, json]() {
                processPacket(socket, json);
            });
        }
    }
}

// 业务分发逻辑
void Widget::processPacket(QTcpSocket *socket, const QJsonObject &json)
{
    QString type = json["type"].toString();

    if (type == "login") {
        check_login(json["phone"].toString(), json["password"].toString(), socket);
    } else if (type == "ping") {
        QJsonObject pong;
        pong["type"] = "pong";
        sendPacket(socket, pong);
    } else if (type == "identify") {
        QString userId = json["userId"].toString().trimmed();
        QMutexLocker locker(&m_onlineClientsMutex);
        m_onlineClients[userId] = socket;
        qDebug() << "用户已识别并建立长连接映射:" << userId;
    } else if (type == "register") {
        // 简化移植注册逻辑
        QJsonObject result;
        QString phone = json["phone"].toString();
        QString name = json["name"].toString();
        QString password = json["password"].toString();

        if (phone.length() != 11 || isPhoneExists(phone)) {
            result["success"] = false;
            result["error"] = "手机号不合法或已存在";
        } else {
            QString userId = generateUserId();
            if (registerUser(userId, name, encryptPassword(password), phone, 0)) {
                result["success"] = true;
                result["userId"] = userId;
            } else {
                result["success"] = false;
                result["error"] = "注册失败";
            }
        }
        result["type"] = "register_res";
        sendPacket(socket, result);
    } else if (type == "send_message") {
        handleSendMessage(json, socket);
    } else if (type == "get_all_users") {
        QSqlDatabase db = ConnectionPool::openConnection();
        QJsonObject result;
        QJsonArray userArray;
        if (db.isOpen()) {
            QSqlQuery query(db);
            if (query.exec("SELECT id, name, phone FROM user")) {
                while (query.next()) {
                    QJsonObject userObj;
                    userObj["user_id"] = query.value("id").toString();
                    userObj["name"] = query.value("name").toString();
                    userObj["phone"] = query.value("phone").toString();
                    userArray.append(userObj);
                }
                result["success"] = true;
                result["users"] = userArray;
            } else {
                result["success"] = false;
            }
        } else {
            result["success"] = false;
        }
        ConnectionPool::closeConnection(db);
        result["type"] = "get_all_users_res";
        sendPacket(socket, result);
    } else if (type == "get_allAvatar") {
        QJsonObject result;
        QJsonArray avatarArray;
        QSqlDatabase db = ConnectionPool::openConnection();
        if (db.isOpen()) {
            QSqlQuery query(db);
            if (query.exec("SELECT id, name FROM user")) {
                while (query.next()) {
                    QJsonObject obj;
                    obj["user_id"] = query.value("id").toString();
                    obj["name"] = query.value("name").toString();
                    avatarArray.append(obj);
                }
            }
        }
        ConnectionPool::closeConnection(db);
        result["success"] = true;
        result["avatars"] = avatarArray;
        result["type"] = "get_allAvatar_res";
        sendPacket(socket, result);
    } else if (type == "logout") {
        QString phone = json["phone"].toString();
        QString userId = getUserIdByPhone(phone);
        if (!userId.isEmpty()) {
            QMutexLocker locker(&m_onlineClientsMutex);
            m_onlineClients.remove(userId);
        }
        QJsonObject result;
        result["type"] = "logout_res";
        result["success"] = true;
        sendPacket(socket, result);
    } else if (type == "upload_avatar") {
        handleAvatarUpload(json, socket);
    } else if (type == "check_phone") {
        QString phone = json["phone"].toString();
        QJsonObject result;
        result["type"] = "check_phone_res";
        
        QSqlDatabase db = ConnectionPool::openConnection();
        if (db.isOpen()) {
            QSqlQuery query(db);
            query.prepare("SELECT id FROM user WHERE phone = :phone");
            query.bindValue(":phone", phone);

            if (query.exec() && query.next()) {
                result["success"] = true;
                result["status"] = "exist";
                result["id"] = query.value("id").toString();
            } else {
                result["success"] = true;
                result["status"] = "not_exist";
            }
        } else {
            result["success"] = false;
        }
        ConnectionPool::closeConnection(db);
        sendPacket(socket, result);
    } else if (type == "get_avatar") {
        QString userId = json["user_id"].toString();
        QJsonObject result;
        result["type"] = "get_avatar_res";
        result["user_id"] = userId;

        QString avatarPath;
        QSqlDatabase db = ConnectionPool::openConnection();
        if (db.isOpen()) {
            QSqlQuery query(db);
            query.prepare("SELECT avatar_path FROM user WHERE id = ?");
            query.addBindValue(userId);
            if (query.exec() && query.next()) {
                avatarPath = query.value(0).toString();
            }
        }
        ConnectionPool::closeConnection(db);

        QFile file(avatarPath.isEmpty() ? "avatars/default.png" : avatarPath);
        if (file.open(QIODevice::ReadOnly)) {
            QByteArray imageData = file.readAll();
            result["success"] = true;
            result["avatar_data"] = QString(imageData.toBase64());
        } else {
            result["success"] = false;
            result["error"] = "头像文件不存在";
        }
        sendPacket(socket, result);
    } else if (type == "get_messages") {
        QString toUserId = json["toUserId"].toString().trimmed();
        QString fromUserId = json["fromUserId"].toString().trimmed();
        QString lastId = json["lastMessageId"].toString().trimmed();
        if (lastId.isEmpty()) lastId = "0";

        QList<QJsonObject> msgs = queryMessages(fromUserId, toUserId, lastId);
        QJsonArray msgArray;
        for (const auto& msg : msgs) {
            msgArray.append(msg);
        }

        QJsonObject result;
        result["type"] = "get_messages_res";
        result["success"] = true;
        result["toUserId"] = toUserId;
        result["messages"] = msgArray;
        sendPacket(socket, result);
    }
}
void Widget::handleAvatarUpload(const QJsonObject &json, QTcpSocket *socket) {
    QJsonObject result;
    result["type"] = "upload_avatar_res";
    QString userId = json["user_id"].toString();
    QString base64Data = json["avatar_data"].toString();

    if (userId.isEmpty() || base64Data.isEmpty()) {
        result["success"] = false;
        result["error"] = "用户ID或头像数据不能为空";
        sendPacket(socket, result);
        return;
    }

    QByteArray imageData = QByteArray::fromBase64(base64Data.toUtf8());
    QImage image;
    if (!image.loadFromData(imageData)) {
        result["success"] = false;
        result["error"] = "无效的图片数据";
        sendPacket(socket, result);
        return;
    }

    QString avatarDir = "avatars/";
    QDir().mkpath(avatarDir);
    QString filePath = avatarDir + userId + ".png";

    if (image.save(filePath, "PNG")) {
        if (updateAvatarPath(userId, filePath)) {
            result["success"] = true;
            result["message"] = "头像上传成功";
        } else {
            result["success"] = false;
            result["error"] = "更新头像路径失败";
        }
    } else {
        result["success"] = false;
        result["error"] = "图片保存失败";
    }

    sendPacket(socket, result);
}
// 更新用户头像路径到数据库
bool Widget::updateAvatarPath(const QString &userId, const QString &path) {
    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen()) return false;

    QSqlQuery query(db);
    query.prepare("UPDATE user SET avatar_path = :path WHERE id = :id");
    query.bindValue(":path", path);
    query.bindValue(":id", userId);

    bool res = query.exec();
    ConnectionPool::closeConnection(db);
    return res;
}
// 登录验证：检查手机号和密码是否匹配
void Widget::check_login(QString phone, QString password, QTcpSocket* socket)
{
    QSqlDatabase db = ConnectionPool::openConnection();
    QJsonObject result;
    result["type"] = "login_res";

    if (!db.isOpen()) {
        result["success"] = false;
        result["error"] = "数据库连接失败";
        sendPacket(socket, result);
        return;
    }

    QSqlQuery query(db);
    query.prepare("SELECT id, name, password, phone FROM user WHERE phone = :phone");
    query.bindValue(":phone", phone);

    if (query.exec() && query.next()) {
        QString dbPwd = query.value("password").toString();
        QString encryptedInput = encryptPassword(password);

        if (dbPwd == encryptedInput) {
            QString userId = query.value("id").toString();
            QString userName = query.value("name").toString();
            QString userPhone = query.value("phone").toString();

            result["success"] = true;
            result["userId"] = userId;
            result["name"] = userName;
            result["phone"] = userPhone;
        } else {
            result["success"] = false;
            result["error"] = "密码错误";
        }
    } else {
        result["success"] = false;
        result["error"] = "手机号未注册";
    }

    ConnectionPool::closeConnection(db);
    sendPacket(socket, result);
}

// 发送封装后的 TCP 数据包：[4字节长度][JSON数据]
void Widget::sendPacket(QTcpSocket *socket, const QJsonObject &json)
{
    if (!socket) return;

    // 线程安全发送：如果不在主线程，通过 invokeMethod 切换
    if (QThread::currentThread() != this->thread()) {
        QMetaObject::invokeMethod(this, [this, socket, json]() {
            sendPacket(socket, json);
        }, Qt::QueuedConnection);
        return;
    }

    // 主线程中检查：即使 socket 正准备销毁，只要 deleteLater 还没执行完，指针依然有效
    if (socket->state() != QAbstractSocket::ConnectedState) return;

    QByteArray jsonData = QJsonDocument(json).toJson(QJsonDocument::Compact);
    quint32 size = jsonData.size();

    QByteArray packet;
    QDataStream ds(&packet, QIODevice::WriteOnly);
    ds << size; // 写入 4 字节长度（大端）
    packet.append(jsonData);

    socket->write(packet);
    socket->flush();// 确保数据立即发送
}

// 数据库管理转移至 ConnectionPool

// 密码加密：使用SHA256算法加密密码
QString Widget::encryptPassword(const QString &password)
{
    QCryptographicHash hash (QCryptographicHash::Sha256);
    hash.addData (password.toUtf8 ());  // 添加原始密码数据
    return QString (hash.result ().toHex ());  // 转换为十六进制字符串返回
}

// 检查手机号是否已注册
bool Widget::isPhoneExists(const QString &phone)
{
    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen ()) {
        return false;
    }

    // 使用预处理语句防止SQL注入
    QSqlQuery query(db);
    query.prepare("SELECT COUNT(*) FROM user WHERE phone = :phone");
    query.bindValue(":phone", phone);

    // 执行查询
    bool exists = false;
    if (query.exec() && query.next ()) {
        exists = query.value(0).toInt() > 0;
    } else {
        qWarning () << "查询手机号失败:" << query.lastError ().text();
    }

    ConnectionPool::closeConnection(db);
    return exists;
}

// 生成用户ID：基于当前用户数量生成4位数字ID（不足补0）
QString Widget::generateUserId()
{
    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen ()) {
        qWarning() << "generateUserId: 数据库未打开";
        return "";
    }

    int count = 0;
    QSqlQuery query(db);
    if (query.exec("SELECT COUNT(*) FROM user") && query.next()) {
        count = query.value(0).toInt();
    }

    ConnectionPool::closeConnection(db);
    // 生成4位数字ID（从0000开始递增）
    return QString("%1").arg(count+1, 4, 10, QLatin1Char('0'));
}

// 注册用户：将用户信息插入数据库
bool Widget::registerUser(const QString &id, const QString &name, const QString &password, const QString &phone,const int online)
{
    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen()) {
        return false;
    }

    QSqlQuery query(db);
    query.prepare("INSERT INTO user (id, name, password, phone,online) VALUES (:id, :name, :password, :phone,:online)");
    query.bindValue(":id", id);
    query.bindValue(":name", name);
    query.bindValue(":password", password);
    query.bindValue(":phone", phone);
    query.bindValue(":online", online);

    bool success = query.exec();
    ConnectionPool::closeConnection(db);
    return success;
}
// 辅助函数：通过手机号查询用户ID
QString Widget::getUserIdByPhone(const QString& phone) {
    QSqlDatabase db = ConnectionPool::openConnection();
    QString id;
    if (db.isOpen()) {
        QSqlQuery query(db);
        query.prepare("SELECT id FROM user WHERE phone = ?");
        query.addBindValue(phone);

        if (query.exec() && query.next()) {
            id = query.value(0).toString();
        }
    }
    ConnectionPool::closeConnection(db);
    return id;
}

// 核心功能：通过手机号发送好友请求
QJsonObject Widget::sendFriendRequestByPhone(const QString& fromUserId, const QString& toPhone) {
    QJsonObject result;

    // 1. 检查目标手机号是否存在
    QString toUserId = getUserIdByPhone(toPhone);
    if (toUserId.isEmpty()) {
        result["success"] = false;
        result["message"] = "目标手机号未注册";
        return result;
    }

    // 2. 检查是否添加自己
    if (fromUserId == toUserId) {
        result["success"] = false;
        result["message"] = "不能添加自己为好友";
        return result;
    }

    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen()) {
        result["success"] = false;
        result["message"] = "数据库连接失败";
        return result;
    }

    QSqlQuery query(db);
    // 3. 检查是否已成为好友
    query.prepare("SELECT id FROM friend WHERE (user_id = ? AND friend_id = ?)");
    query.addBindValue(fromUserId);
    query.addBindValue(toUserId);
    if (query.exec() && query.next()) {
        result["success"] = false;
        result["message"] = "对方已是你的好友";
        ConnectionPool::closeConnection(db);
        return result;
    }

    // 4. 检查是否有未处理的请求
    query.prepare("SELECT id FROM friend_request WHERE from_user_id = ? AND to_user_id = ? AND status = 0");
    query.addBindValue(fromUserId);
    query.addBindValue(toUserId);
    if (query.exec() && query.next()) {
        result["success"] = false;
        result["message"] = "已发送好友请求，请等待回复";
        ConnectionPool::closeConnection(db);
        return result;
    }

    // 5. 插入新的好友请求
    query.prepare("INSERT INTO friend_request (from_user_id, to_user_id) VALUES (?, ?)");
    query.addBindValue(fromUserId);
    query.addBindValue(toUserId);
    if (query.exec()) {
        result["success"] = true;
        result["message"] = "好友请求发送成功";
        result["request_id"] = query.lastInsertId().toString();
    } else {
        result["success"] = false;
        result["message"] = "发送请求失败，请重试";
    }

    ConnectionPool::closeConnection(db);
    return result;
}

// 处理好友请求（同意/拒绝）
QJsonObject Widget::handleFriendRequest(const QString& reqId, int status) {
    QJsonObject result;
    if (status != 1 && status != 2) { // 1-同意 2-拒绝
        result["success"] = false;
        result["message"] = "无效的请求状态";
        return result;
    }

    QSqlDatabase db = ConnectionPool::openConnection();
    if (!db.isOpen()) {
        result["success"] = false;
        result["message"] = "数据库连接失败";
        return result;
    }

    QSqlQuery query(db);

    // 1. 查询请求是否存在
    query.prepare("SELECT from_user_id, to_user_id FROM friend_request WHERE id = ? AND status = 0");
    query.addBindValue(reqId);
    if (!query.exec() || !query.next()) {
        result["success"] = false;
        result["message"] = "好友请求不存在或已处理";
        ConnectionPool::closeConnection(db);
        return result;
    }

    QString fromUserId = query.value(0).toString();
    QString toUserId = query.value(1).toString();

    // 2. 更新请求状态
    query.prepare("UPDATE friend_request SET status = ? WHERE id = ?");
    query.addBindValue(status);
    query.addBindValue(reqId);
    if (!query.exec()) {
        result["success"] = false;
        result["message"] = "处理请求失败";
        ConnectionPool::closeConnection(db);
        return result;
    }

    // 3. 如果同意，添加双向好友关系
    if (status == 1) {
        // 插入正向关系
        query.prepare("INSERT INTO friend (user_id, friend_id) VALUES (?, ?)");
        query.addBindValue(toUserId);
        query.addBindValue(fromUserId);
        if (!query.exec()) {
            result["success"] = false;
            result["message"] = "添加好友失败";
            ConnectionPool::closeConnection(db);
            return result;
        }

        // 插入反向关系（便于双向查询）
        query.prepare("INSERT INTO friend (user_id, friend_id) VALUES (?, ?)");
        query.addBindValue(fromUserId);
        query.addBindValue(toUserId);
        query.exec(); // 允许轻微失败，不影响主流程
    }

    ConnectionPool::closeConnection(db);
    result["success"] = true;
    result["message"] = status == 1 ? "已同意好友请求" : "已拒绝好友请求";
    return result;
}

// 获取用户的好友列表（包含昵称）
QJsonObject Widget::getFriendList(const QString& userId) {
    QJsonObject result;
    QJsonArray friendArray;

    QSqlDatabase db = ConnectionPool::openConnection();
    if (db.isOpen()) {
        QSqlQuery query(db);
        query.prepare("SELECT f.friend_id, u.name, u.phone "
                      "FROM friend f "
                      "JOIN user u ON f.friend_id = u.id "
                      "WHERE f.user_id = ?");
        query.addBindValue(userId);

        if (query.exec()) {
            while (query.next()) {
                QJsonObject friendObj;
                friendObj["user_id"] = query.value(0).toString();
                friendObj["name"] = query.value(1).toString();
                friendObj["phone"] = query.value(2).toString(); // 可根据隐私需求决定是否返回
                friendArray.append(friendObj);
            }
            result["success"] = true;
            result["friends"] = friendArray;
        } else {
            result["success"] = false;
            result["message"] = "获取好友列表失败";
        }
    } else {
        result["success"] = false;
        result["message"] = "数据库连接失败";
    }

    ConnectionPool::closeConnection(db);
    return result;
}
