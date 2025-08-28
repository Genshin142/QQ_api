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

// 数据库配置参数（实际使用时建议从配置文件读取，避免硬编码）
#define DB_HOST "localhost"    // 数据库主机地址
#define DB_PORT 3306           // 数据库端口
#define DB_NAME "load_data"// 数据库名称
#define DB_USER "root"         // 数据库用户名
#define DB_PASS "362345943"// 数据库密码

// 静态成员初始化
QSqlDatabase Widget::dbPool;
QMutex Widget::dbMutex;
QQueue<QString> Widget::logQueue;
QMutex Widget::logMutex;
QWaitCondition Widget::logCond;
bool Widget::logRunning = true;

Widget::Widget(QWidget *parent)
    : QWidget(parent)
    , ui(new Ui::Widget)
{
    ui->setupUi(this);
    writeLog("===== 服务器启动初始化 =====", WIDGET_LOG_INFO);

    // 检查MySQL驱动是否可用
    if (!QSqlDatabase::isDriverAvailable ("QMYSQL")) {
        QString err = "未找到MySQL驱动，请安装Qt的MySQL插件";
        writeLog(err, WIDGET_LOG_ERROR);
        return ;
    }
    writeLog("MySQL驱动检查通过", WIDGET_LOG_INFO);

    // 初始化数据库连接池
    {
        QMutexLocker locker(&dbMutex);
        dbPool = QSqlDatabase::addDatabase("QMYSQL", "global_conn");
        dbPool.setHostName(DB_HOST);
        dbPool.setPort(DB_PORT);
        dbPool.setDatabaseName(DB_NAME);
        dbPool.setUserName(DB_USER);
        dbPool.setPassword(DB_PASS);

        if (!dbPool.open()) {
            writeLog("数据库连接池初始化失败：" + dbPool.lastError().text(), WIDGET_LOG_ERROR);
        } else {
            writeLog("数据库连接池初始化成功，连接名：global_conn", WIDGET_LOG_INFO);
        }
    }

    // 启动日志工作线程
    QThread* logThread = new QThread;
    QObject::connect(logThread, &QThread::started, &Widget::logWorker);
    QObject::connect(this, &Widget::destroyed, [logThread]() {
        logRunning = false;
        logCond.wakeOne();
        logThread->quit();
        logThread->wait();
        delete logThread;
        writeLog("日志工作线程已停止", WIDGET_LOG_INFO);
    });
    logThread->start();
    writeLog("日志工作线程启动成功", WIDGET_LOG_INFO);

    server = new QTcpServer(this);
    // 创建TCP服务器并监听8080端口（任意地址）
    if (!server->listen (QHostAddress::Any, 8080)) {
        QString err = "无法启动服务器: " + server->errorString();
        writeLog(err, WIDGET_LOG_ERROR);
        return ;
    }

    // 服务器启动成功日志
    writeLog("服务器启动成功，监听地址：0.0.0.0:8080", WIDGET_LOG_INFO);
    writeLog("注册接口：POST /api/register", WIDGET_LOG_INFO);
    writeLog("登录接口：POST /api/login", WIDGET_LOG_INFO);
    writeLog("登出接口：POST /api/logout", WIDGET_LOG_INFO);
    writeLog("好友功能接口：GET /api/get_friend_list", WIDGET_LOG_INFO);
    writeLog("发送消息接口：POST /api/send_message", WIDGET_LOG_INFO);
    writeLog("获取消息接口：GET /api/get_messages", WIDGET_LOG_INFO);  // 补充获取消息接口
    writeLog("头像上传接口：POST /api/upload_avatar", WIDGET_LOG_INFO);  // 新增头像上传接口日志
    writeLog("头像获取接口：GET /api/get_avatar", WIDGET_LOG_INFO);      // 新增头像获取接口日志



    // 处理新客户端连接
    QObject::connect(server, &QTcpServer::newConnection, [&]() {
        while (QTcpSocket* socket = server->nextPendingConnection()) {
            QString clientInfo = QString("新客户端连接：%1:%2")
                                     .arg(socket->peerAddress().toString())
                                     .arg(socket->peerPort());
            writeLog(clientInfo, WIDGET_LOG_INFO);

            // 绑定数据接收信号
            QObject::connect(socket, &QTcpSocket::readyRead, [this, socket]() {
                this->handleRequest(socket);
            });

            // 绑定断开连接信号
            QObject::connect(socket, &QTcpSocket::disconnected, [socket]() {
                QString disconnInfo = QString("客户端断开连接：%1:%2")
                                          .arg(socket->peerAddress().toString())
                                          .arg(socket->peerPort());
                Widget::writeLog(disconnInfo, WIDGET_LOG_INFO);
                socket->deleteLater(); // 延迟释放资源
            });
        }
    });

}

Widget::~Widget() {
    // 服务器关闭前停止监听
    if (server->isListening()) {
        server->close();
        writeLog("服务器已停止监听端口", WIDGET_LOG_INFO);
    }
    // 关闭数据库连接
    {
        QMutexLocker locker(&dbMutex);
        if (dbPool.isOpen()) {
            dbPool.close();
            writeLog("数据库连接已关闭", WIDGET_LOG_INFO);
        }
    }
    writeLog("===== 服务器程序已退出 =====", WIDGET_LOG_INFO);
    delete server;  // 释放服务器对象
    delete ui;
}

// 日志工作线程（单独线程处理文件写入）
void Widget::logWorker() {
    writeLog("日志工作线程启动，开始处理日志队列", WIDGET_LOG_INFO);
    while (logRunning) {
        QMutexLocker locker(&logMutex);
        logCond.wait(&logMutex); // 等待日志写入信号

        // 构建根目录下的"日志"文件夹路径
        QString logDirPath = QCoreApplication::applicationDirPath() + "/日志";
        // 检查并创建"日志"文件夹（不存在则创建）
        QDir logDir(logDirPath);
        if (!logDir.exists() && !logDir.mkpath(".")) {
            qWarning() << "无法创建日志文件夹：" << logDirPath;
            continue; // 创建失败则跳过本轮写入
        }

        // 获取当前日期并生成带日期的文件名（日志文件夹下的server_log_日期.log）
        QString dateStr = QDateTime::currentDateTime().toString("yyyyMMdd");
        QString fileName = logDirPath + QString("/server_log_%1.log").arg(dateStr);
        QFile file(fileName);

        // 批量写入队列中的日志
        while (!logQueue.isEmpty()) {
            if (file.open(QIODevice::WriteOnly | QIODevice::Append | QIODevice::Text)) {
                QTextStream out(&file);
                out << logQueue.dequeue() << "\n";
                file.close();
            } else {
                qWarning() << "无法打开服务器日志文件:" << fileName << "，错误：" << file.errorString();
                break; // 打开失败则终止本轮写入
            }
        }
    }
    writeLog("日志工作线程退出", WIDGET_LOG_INFO);
}



// 实现日志函数：输出日志到控制台和异步写入文件
void Widget::writeLog(const QString& message, LogType type) {
    // 获取当前时间字符串（精确到毫秒）
    QString timeStr = QDateTime::currentDateTime().toString("yyyy-MM-dd hh:mm:ss.zzz");

    // 根据日志类型转换为对应字符串
    QString typeStr;
    switch (type) {
    case WIDGET_LOG_INFO: typeStr = "INFO"; break;
    case WIDGET_LOG_WARNING: typeStr = "WARNING"; break;
    case WIDGET_LOG_ERROR: typeStr = "ERROR"; break;
    }

    // 构造完整日志内容
    QString logStr = QString("[%1] [%2] WidgetServer: %3")
                         .arg(timeStr)
                         .arg(typeStr)
                         .arg(message);

    // 输出到控制台
    qDebug() << logStr;

    // 日志入队，唤醒工作线程
    QMutexLocker locker(&logMutex);
    logQueue.enqueue(logStr);
    logCond.wakeOne();
}
// 实现发送消息处理函数
void Widget::handleSendMessage(const QJsonObject& requestData, QTcpSocket* socket)
{
    // 提取消息参数
    QString fromUserId = requestData["fromUserId"].toString();
    QString toUserId = requestData["toUserId"].toString();
    QString content = requestData["content"].toString();

    // 验证必要参数
    if (fromUserId.isEmpty() || toUserId.isEmpty() || content.isEmpty()) {
        QJsonObject response;
        response["success"] = false;
        response["error"] = "缺少必要参数";
        sendResponse(socket, response, 400);
        return;
    }

    // 保存消息到数据库
    bool success = saveMessage(fromUserId, toUserId, content);

    // 构建响应
    QJsonObject response;
    if (success) {
        response["success"] = true;
        response["message"] = "消息发送成功";
        writeLog(QString("消息发送成功：%1 -> %2，内容：%3").arg(fromUserId).arg(toUserId).arg(content));
        sendResponse(socket, response, 200);
    } else {
        response["success"] = false;
        response["error"] = "消息发送失败";
        writeLog(QString("消息发送失败：%1 -> %2").arg(fromUserId).arg(toUserId), WIDGET_LOG_ERROR);
        sendResponse(socket, response, 500);
    }
}

// 实现消息保存到数据库的函数
bool Widget::saveMessage(const QString& fromUserId, const QString& toUserId, const QString& content)
{
    QMutexLocker locker(&dbMutex);

    if (!dbPool.isOpen()) {
        writeLog("数据库连接已关闭，无法保存消息", WIDGET_LOG_ERROR);
        return false;
    }

    QSqlQuery query(dbPool);

    query.prepare("INSERT INTO messages (from_user_id, to_user_id, content, send_time) "
                  "VALUES (:from, :to, :content, DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s'))");
    query.bindValue(":from", fromUserId);
    query.bindValue(":to", toUserId);
    query.bindValue(":content", content);

    if (!query.exec()) {
        writeLog(QString("保存消息到数据库失败：%1").arg(query.lastError().text()), WIDGET_LOG_ERROR);
        return false;
    }

    return true;
}

QList<QJsonObject> Widget::queryMessages(const QString &m_myUserId, const QString &toUserId, const QString &m_lastMessageId)
{
    QMutexLocker locker(&dbMutex);
    QList<QJsonObject> messages;

    if (!dbPool.isOpen()) {
        writeLog("数据库连接已关闭，无法查询消息", WIDGET_LOG_ERROR);
        return messages;
    }

    QSqlQuery query(dbPool);
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
        writeLog(QString("查询消息失败：%1").arg(query.lastError().text()), WIDGET_LOG_ERROR);
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

    writeLog(QString("查询到 %1 条新消息，%2 与 %3 的对话").arg(messages.size()).arg(m_myUserId).arg(toUserId), WIDGET_LOG_INFO);
    return messages;
}
// 处理客户端请求：解析HTTP请求并分发到对应处理逻辑
void Widget::handleRequest(QTcpSocket *socket)
{

    // 读取客户端发送的所有数据
    QByteArray data = socket->readAll();
    QString request = QString::fromUtf8(data);
    // 日志只显示前100字符，避免长数据刷屏
    writeLog("收到原始请求数据（前100字符）：" + request.left(100) + "...", WIDGET_LOG_INFO);

    // 解析HTTP请求方法（GET/POST等）和路径
    QString method = request.left(request.indexOf(' '));
    QString path = request.mid(request.indexOf(' ') + 1);
    path = path.left(path.indexOf(' '));

    writeLog("收到请求: " + method + " " + path, WIDGET_LOG_INFO);
    qDebug () << "收到请求:" << method << path;

    // 处理注册请求（POST /api/register）
    if (method == "POST" && path == "/api/register") {
        writeLog("开始处理用户注册请求", WIDGET_LOG_INFO);

        // 提取请求中的JSON数据（从{开始到}结束）
        int jsonStart = request.indexOf ('{');
        int jsonEnd = request.lastIndexOf ('}') + 1;

        // 检查JSON格式是否完整
        if (jsonStart == -1 || jsonEnd == -1) {
            writeLog("注册请求：无效的JSON格式（未找到完整的{}）", WIDGET_LOG_WARNING);
            QString response = "HTTP/1.1 400 Bad Request\r\n"
                               "Content-Type: application/json\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "\r\n"
                               "{\"success\": false,\"error\":\"无效的 JSON 格式\"}";
            socket->write(response.toUtf8());
            socket->flush();
            socket->disconnectFromHost();
            return;
        }

        // 解析JSON数据
        QByteArray jsonData = request.mid(jsonStart, jsonEnd - jsonStart).toUtf8();
        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (doc.isNull()) {
            writeLog("注册请求：JSON解析失败（格式错误）", WIDGET_LOG_WARNING);
            QString response = "HTTP/1.1 400 Bad Request\r\n"
                               "Content-Type: application/json\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "\r\n"
                               "{\"success\": false,\"error\": \"JSON 解析失败\"}";
            socket->write(response.toUtf8());
            socket->flush();
            socket->disconnectFromHost();
            return;
        }

        // 提取JSON中的注册信息
        QJsonObject json = doc.object();
        QString phone = json["phone"].toString();
        QString name = json["name"].toString();
        QString password = json["password"].toString();
        writeLog("注册请求：手机号=" + phone + ", 昵称=" + name, WIDGET_LOG_INFO);

        // 验证输入合法性
        QJsonObject result;
        if (phone.isEmpty() || phone.length() != 11) {
            writeLog("注册验证失败：手机号格式错误（需11位），手机号=" + phone, WIDGET_LOG_WARNING);
            result["success"] = false;
            result["error"] = "请输入有效的11位手机号";
        }
        else if (name.isEmpty()) {
            writeLog("注册验证失败：昵称为空，手机号=" + phone, WIDGET_LOG_WARNING);
            result["success"] = false;
            result["error"] = "昵称不能为空";
        }
        else if (password.isEmpty() || password.length() < 8 || password.length() > 16) {
            writeLog("注册验证失败：密码长度不符合要求（8-16位），手机号=" + phone, WIDGET_LOG_WARNING);
            result["success"] = false;
            result["error"] = "密码必须为8-16位";
        }
        else if (isPhoneExists(phone)) {
            writeLog("注册验证失败：手机号已注册，手机号=" + phone, WIDGET_LOG_WARNING);
            result["success"] = false;
            result["error"] = "该手机号已被注册";
        }
        else {
            // 生成用户ID并加密密码
            QString userId = generateUserId();
            if (userId.isEmpty()) {
                writeLog("注册失败：生成用户ID失败，手机号=" + phone, WIDGET_LOG_ERROR);
                result["success"] = false;
                result["error"] = "生成用户 ID 失败";
            } else {
                QString encryptedPwd = encryptPassword(password);
                // 执行注册逻辑
                int online=0;
                if (registerUser(userId, name, encryptedPwd, phone,online)) {
                    writeLog("注册成功：用户ID=" + userId + ", 手机号=" + phone, WIDGET_LOG_INFO);
                    result["success"] = true;
                    result["message"] = "注册成功";
                    result["userId"] = userId;
                } else {
                    writeLog("注册失败：数据库插入失败，手机号=" + phone, WIDGET_LOG_ERROR);
                    result["success"] = false;
                    result["error"] = "注册失败，请重试";
                }
            }
        }

        // 发送注册响应
        sendResponse(socket, result, 200);
    }
    // 处理预检请求（跨域支持，OPTIONS /api/register）
    else if (method == "OPTIONS" && path == "/api/register") {
        writeLog("收到跨域预检请求：OPTIONS /api/register", WIDGET_LOG_INFO);
        // 返回跨域许可响应
        QString response = "HTTP/1.1 200 OK\r\n"
                           "Access-Control-Allow-Origin: *\r\n"
                           "Access-Control-Allow-Methods: POST, OPTIONS\r\n"
                           "Access-Control-Allow-Headers: Content-Type\r\n"
                           "Content-Length: 0\r\n"
                           "\r\n";
        socket->write (response.toUtf8 ());
        writeLog("跨域预检响应已发送", WIDGET_LOG_INFO);
        socket->flush();
        socket->disconnectFromHost();
    }
    // 处理登录请求（POST /api/login）
    else if (method == "POST" && path == "/api/login")
    {
        writeLog("开始处理登录请求", WIDGET_LOG_INFO);

        // 提取请求中的JSON数据
        int jsonStart = request.indexOf ('{');
        int jsonEnd = request.lastIndexOf ('}') + 1;

        // 检查JSON格式是否完整
        if (jsonStart == -1 || jsonEnd == -1) {
            writeLog("登录请求：无效的JSON格式（未找到完整的{}）", WIDGET_LOG_WARNING);
            QString response = "HTTP/1.1 400 Bad Request\r\n"
                               "Content-Type: application/json\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "\r\n"
                               "{\"success\": false,\"error\":\"无效的 JSON 格式\"}";
            socket->write(response.toUtf8());
            socket->flush();
            socket->disconnectFromHost();
            return;
        }

        // 解析JSON数据
        QByteArray jsonData = request.mid(jsonStart, jsonEnd - jsonStart).toUtf8();
        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (doc.isNull()) {
            writeLog("登录请求：JSON解析失败（格式错误）", WIDGET_LOG_WARNING);
            QString response = "HTTP/1.1 400 Bad Request\r\n"
                               "Content-Type: application/json\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "\r\n"
                               "{\"success\": false,\"error\": \"JSON 解析失败\"}";
            socket->write(response.toUtf8());
            socket->flush();
            socket->disconnectFromHost();
            return;
        }

        // 提取登录信息
        QJsonObject json = doc.object();
        QString phone = json["phone"].toString();
        QString password = json["password"].toString();
        writeLog("登录请求：手机号=" + phone, WIDGET_LOG_INFO);

        // 验证手机号和密码
        check_login(phone,password,socket);
    }
    // 在 handleRequest 函数中添加对登出接口的处理
    else if (method == "POST" && path == "/api/logout") {
        writeLog("开始处理登出请求", WIDGET_LOG_INFO);

        int jsonStart = request.indexOf('{');
        int jsonEnd = request.lastIndexOf('}') + 1;
        if (jsonStart == -1 || jsonEnd == -1) {
            // 无效JSON处理（略）
            return;
        }

        QByteArray jsonData = request.mid(jsonStart, jsonEnd - jsonStart).toUtf8();
        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (doc.isNull()) {
            // JSON解析失败处理（略）
            return;
        }

        QJsonObject json = doc.object();
        QString phone = json["phone"].toString();

        QSqlDatabase db = getDatabase();
        db.transaction();
        QSqlQuery query(db);
        query.prepare("UPDATE user SET online = 0 WHERE phone = :phone");
        query.bindValue(":phone", phone);

        QJsonObject result;
        if (query.exec() && query.numRowsAffected() > 0) {
            db.commit();
            writeLog("登出成功：手机号=" + phone, WIDGET_LOG_INFO);
            result["success"] = true;
            result["message"] = "登出成功";
        } else {
            db.rollback();
            writeLog("登出失败：手机号=" + phone + "，错误=" + query.lastError().text(), WIDGET_LOG_ERROR);
            result["success"] = false;
            result["error"] = "登出失败";
        }
        sendResponse(socket, result, 200);
    }
    // 处理获取所有用户信息请求（供内部通讯显示所有用户）
    else if (method == "GET" && path == "/api/get_all_users") {
        writeLog("收到获取所有用户信息请求", WIDGET_LOG_INFO);

        QSqlDatabase db = getDatabase();
        QJsonObject result;
        QJsonArray userArray;

        if (!db.isOpen()) {
            writeLog("获取所有用户失败：数据库连接未打开", WIDGET_LOG_ERROR);
            result["success"] = false;
            result["error"] = "数据库连接失败";
            sendResponse(socket, result, 500);
            return;
        }

        QSqlQuery query(db);
        // 查询所有用户的基本信息（排除密码等敏感字段）
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
            writeLog(QString("成功获取所有用户信息，共 %1 条记录").arg(userArray.size()), WIDGET_LOG_INFO);
        } else {
            writeLog("获取所有用户失败：" + query.lastError().text(), WIDGET_LOG_ERROR);
            result["success"] = false;
            result["error"] = "查询用户信息失败";
        }

        sendResponse(socket, result, 200);
        return;
    }

    // 处理跨域预检请求（针对获取所有用户接口）
    else if (method == "OPTIONS" && path == "/api/get_all_users") {
        writeLog("收到跨域预检请求：OPTIONS /api/get_all_users", WIDGET_LOG_INFO);
        QString response = "HTTP/1.1 200 OK\r\n"
                           "Access-Control-Allow-Origin: *\r\n"
                           "Access-Control-Allow-Methods: GET, OPTIONS\r\n"
                           "Access-Control-Allow-Headers: Content-Type\r\n"
                           "Content-Length: 0\r\n"
                           "\r\n";
        socket->write(response.toUtf8());
        socket->flush();
        socket->disconnectFromHost();
        return;
    }
    // 新增：处理发送消息请求（POST /api/send_message）
    else if (method == "POST" && path == "/api/send_message") {
        writeLog("开始处理发送消息请求", WIDGET_LOG_INFO);

        // 提取请求中的JSON数据
        int jsonStart = request.indexOf('{');
        int jsonEnd = request.lastIndexOf('}') + 1;

        // 检查JSON格式是否完整
        if (jsonStart == -1 || jsonEnd == -1) {
            writeLog("发送消息请求：无效的JSON格式（未找到完整的{}）", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "无效的JSON格式";
            sendResponse(socket, response, 400);
            return;
        }

        // 解析JSON数据
        QByteArray jsonData = request.mid(jsonStart, jsonEnd - jsonStart).toUtf8();
        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (doc.isNull()) {
            writeLog("发送消息请求：JSON解析失败（格式错误）", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "JSON解析失败";
            sendResponse(socket, response, 400);
            return;
        }

        // 调用消息处理函数
        handleSendMessage(doc.object(), socket);
    }
    // 新增：处理获取消息请求（GET /api/get_messages）
    else if (method == "GET" && path.startsWith("/api/get_messages")) {
        writeLog("开始处理获取消息请求", WIDGET_LOG_INFO);

        // 正确解析URL中的查询参数
        QUrl url(path); // 将路径转换为QUrl
        QUrlQuery queryParams(url); // 通过QUrl解析参数

        // 提取参数（无需手动解析路径）
        QString toUserId = queryParams.queryItemValue("toUserId");
        QString m_myUserId = queryParams.queryItemValue("m_myUserId");
        QString m_lastMessageId = queryParams.queryItemValue("m_lastMessageId");
        // 如果参数不存在，设置默认值为 "0"
        if (m_lastMessageId.isEmpty()) {
            m_lastMessageId = "0";
        }

        // 区分群聊和单聊的参数验证逻辑
        if (toUserId.isEmpty()) {
            // 所有场景都必须提供toUserId
            writeLog("获取消息请求：toUserId参数为空", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "toUserId参数为空";
            sendResponse(socket, response, 400);
            return;
        } else if (toUserId != "0000" && m_myUserId.isEmpty()) {
            // 单聊场景必须提供m_myUserId
            writeLog("获取消息请求：单聊场景m_myUserId参数为空", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "单聊场景m_myUserId参数为空";
            sendResponse(socket, response, 400);
            return;
        }

        // 查询消息并返回（调用修正后的queryMessages）
        QList<QJsonObject> messages = queryMessages(m_myUserId, toUserId, m_lastMessageId);
        QJsonArray msgArray;
        for (const auto& msg : messages) {
            msgArray.append(msg);
        }

        QJsonObject response;
        response["success"] = true;
        response["messages"] = msgArray;
        sendResponse(socket, response, 200);
    }
    // 处理头像上传请求（POST /api/upload_avatar）
    else if (method == "POST" && path == "/api/upload_avatar") {
        writeLog("开始处理头像上传请求", WIDGET_LOG_INFO);

        // 提取请求中的JSON数据
        int jsonStart = request.indexOf('{');
        int jsonEnd = request.lastIndexOf('}') + 1;

        if (jsonStart == -1 || jsonEnd == -1) {
            writeLog("头像上传请求：无效的JSON格式", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "无效的JSON格式";
            sendResponse(socket, response, 400);
            return;
        }

        QByteArray jsonData = request.mid(jsonStart, jsonEnd - jsonStart).toUtf8();
        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (doc.isNull()) {
            writeLog("头像上传请求：JSON解析失败", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "JSON解析失败";
            sendResponse(socket, response, 400);
            return;
        }

        handleAvatarUpload(doc.object(), socket);
    }
    // 处理头像获取请求（GET /api/get_avatar）
    else if (method == "GET" && path.startsWith("/api/get_avatar")) {
        writeLog("开始处理头像获取请求", WIDGET_LOG_INFO);

        QUrl url(path);
        QUrlQuery queryParams(url);
        QString userId = queryParams.queryItemValue("user_id");

        if (userId.isEmpty()) {
            QString response = "HTTP/1.1 400 Bad Request\r\n"
                               "Content-Type: application/json\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "\r\n"
                               "{\"success\": false,\"error\":\"user_id参数不能为空\"}";
            socket->write(response.toUtf8());
            socket->flush();
            socket->disconnectFromHost();
            return;
        }

        // 从数据库查询头像路径
        QString avatarPath;
        {
            QMutexLocker locker(&dbMutex);
            QSqlQuery query(dbPool);
            query.prepare("SELECT avatar_path FROM user WHERE id = ?");
            query.addBindValue(userId);
            if (query.exec() && query.next()) {
                avatarPath = query.value(0).toString();
            }
        }

        // 读取图片文件并返回
        QFile file(avatarPath.isEmpty() ? "avatars/default.png" : avatarPath);
        if (file.open(QIODevice::ReadOnly)) {
            QByteArray imageData = file.readAll();
            QString response = "HTTP/1.1 200 OK\r\n"
                               "Content-Type: image/png\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "Content-Length: " + QString::number(imageData.size()) + "\r\n"
                                                                     "\r\n";
            socket->write(response.toUtf8());
            socket->write(imageData);
        } else {
            // 返回默认头像
            QFile defaultFile("avatars/default.png");
            if (defaultFile.open(QIODevice::ReadOnly)) {
                QByteArray defaultData = defaultFile.readAll();
                QString response = "HTTP/1.1 200 OK\r\n"
                                   "Content-Type: image/png\r\n"
                                   "Access-Control-Allow-Origin: *\r\n"
                                   "Content-Length: " + QString::number(defaultData.size()) + "\r\n"
                                                                           "\r\n";
                socket->write(response.toUtf8());
                socket->write(defaultData);
            } else {
                QString response = "HTTP/1.1 404 Not Found\r\n"
                                   "Content-Type: application/json\r\n"
                                   "Access-Control-Allow-Origin: *\r\n"
                                   "\r\n"
                                   "{\"success\": false,\"error\":\"头像不存在\"}";
                socket->write(response.toUtf8());
            }
        }

        socket->flush();
        socket->disconnectFromHost();
    }
    // 处理获取所有用户头像请求（GET /api/get_allAvatar）
    else if (method == "GET" && path == "/api/get_allAvatar") {
        writeLog("开始处理所有用户头像获取请求", WIDGET_LOG_INFO);

        // 从数据库查询所有用户的ID和头像路径
        QList<QPair<QString, QString>> allAvatars;
        {
            QMutexLocker locker(&dbMutex);
            QSqlQuery query(dbPool);
            // 查询所有用户的ID和头像路径
            if (query.exec("SELECT id, avatar_path FROM user")) {
                while (query.next()) {
                    QString userId = query.value(0).toString();
                    QString avatarPath = query.value(1).toString();
                    // 如果用户没有设置头像，使用默认头像路径
                    if (avatarPath.isEmpty()) {
                        avatarPath = "avatars/default.png";
                    }
                    allAvatars.append(qMakePair(userId, avatarPath));
                }
            } else {
                writeLog("查询所有用户头像失败: " + query.lastError().text(), WIDGET_LOG_ERROR);
            }
        }

        // 构建JSON响应
        QJsonArray avatarArray;
        foreach (auto &pair, allAvatars) {
            QJsonObject avatarObj;
            avatarObj["user_id"] = pair.first;
            avatarObj["avatar_path"] = pair.second;
            // 可以添加头像URL字段，方便前端直接使用
            avatarObj["avatar_url"] = "/api/get_avatar?user_id=" + pair.first;
            avatarArray.append(avatarObj);
        }

        QJsonObject responseObj;
        responseObj["success"] = true;
        responseObj["count"] = allAvatars.size();
        responseObj["avatars"] = avatarArray;

        QJsonDocument doc(responseObj);
        QString jsonString = doc.toJson(QJsonDocument::Compact);

        // 发送HTTP响应
        QString response = "HTTP/1.1 200 OK\r\n"
                           "Content-Type: application/json\r\n"
                           "Access-Control-Allow-Origin: *\r\n"
                           "Content-Length: " + QString::number(jsonString.size()) + "\r\n"
                                                                  "\r\n"
                           + jsonString;

        socket->write(response.toUtf8());
        socket->flush();
        socket->disconnectFromHost();
    }
    else if(method == "POST" && path == "/api/check_phone")
    {
        writeLog("开始处理手机号存在性检查请求", WIDGET_LOG_INFO);

        // 提取请求中的JSON数据
        int jsonStart = request.indexOf('{');
        int jsonEnd = request.lastIndexOf('}') + 1;

        // 检查JSON格式是否完整
        if (jsonStart == -1 || jsonEnd == -1) {
            writeLog("手机号检查请求：无效的JSON格式（未找到完整的{}）", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "无效的JSON格式";
            sendResponse(socket, response, 400);
            return;
        }

        // 解析JSON数据
        QByteArray jsonData = request.mid(jsonStart, jsonEnd - jsonStart).toUtf8();
        QJsonDocument doc = QJsonDocument::fromJson(jsonData);
        if (doc.isNull()) {
            writeLog("手机号检查请求：JSON解析失败（格式错误）", WIDGET_LOG_WARNING);
            QJsonObject response;
            response["success"] = false;
            response["error"] = "JSON解析失败";
            sendResponse(socket, response, 400);
            return;
        }

        // 提取手机号参数
        QJsonObject json = doc.object();
        QString phone = json["phone"].toString().trimmed();
        writeLog("手机号检查请求：待验证手机号=" + phone, WIDGET_LOG_INFO);

        // 验证手机号格式（11位数字）
        QJsonObject result;
        // 查询数据库检查手机号是否存在，并获取头像路径
        QMutexLocker locker(&dbMutex);
        QSqlQuery query(dbPool);
        query.prepare("SELECT * FROM user WHERE phone = :phone");
        query.bindValue(":phone", phone);

        if (query.exec() && query.next()) {
            // 手机号存在，返回状态和用户ID
            result["success"] = true;
            result["status"] = "exist";
            result["id"] = query.value("id").toString();  // 返回用户ID
            writeLog("手机号检查结果：已存在（" + phone + "），用户ID：" + result["id"].toString(), WIDGET_LOG_INFO);
        } else {
            // 手机号不存在，返回默认头像路径
            result["success"] = true;
            result["status"] = QJsonValue(QJsonValue::Null);

        }

        // 发送响应
        sendResponse(socket, result, 200);
    }
    // 处理未知请求
    else {
        writeLog("收到未知请求：" + method + " " + path, WIDGET_LOG_WARNING);
        QJsonObject response;
        response["success"] = false;
        response["error"] = "未知接口";
        sendResponse(socket, response, 404);
    }



}
// 处理头像上传核心逻辑
void Widget::handleAvatarUpload(const QJsonObject &json, QTcpSocket *socket) {
    QJsonObject result;
    QString userId = json["user_id"].toString();
    QString base64Data = json["avatar_data"].toString();

    if (userId.isEmpty() || base64Data.isEmpty()) {
        result["success"] = false;
        result["error"] = "用户ID或头像数据不能为空";
        sendResponse(socket, result, 400);
        return;
    }

    // 解码Base64数据
    QByteArray imageData = QByteArray::fromBase64(base64Data.toUtf8());
    QImage image;
    if (!image.loadFromData(imageData)) {
        result["success"] = false;
        result["error"] = "无效的图片数据";
        sendResponse(socket, result, 400);
        return;
    }

    // 保存图片到服务器目录
    QString avatarDir = "avatars/";
    QDir().mkpath(avatarDir); // 确保目录存在
    QString filePath = avatarDir + userId + ".png"; // 用用户ID作为文件名

    if (image.save(filePath, "PNG")) {
        // 更新数据库中的头像路径
        if (updateAvatarPath(userId, filePath)) {
            result["success"] = true;
            result["message"] = "头像上传成功";
            writeLog("用户" + userId + "头像上传成功", WIDGET_LOG_INFO);
        } else {
            result["success"] = false;
            result["error"] = "更新头像路径失败";
        }
    } else {
        result["success"] = false;
        result["error"] = "图片保存失败";
        writeLog("用户" + userId + "头像保存失败", WIDGET_LOG_ERROR);
    }

    sendResponse(socket, result, 200);
}
// 更新用户头像路径到数据库
bool Widget::updateAvatarPath(const QString &userId, const QString &path) {
    QMutexLocker locker(&dbMutex);
    QSqlQuery query(dbPool);

    query.prepare("UPDATE user SET avatar_path = :path WHERE id = :id");
    query.bindValue(":path", path);
    query.bindValue(":id", userId);

    return query.exec();
}
// 登录验证：检查手机号和密码是否匹配
void Widget::check_login(QString phone, QString password, QTcpSocket* socket)
{
    writeLog("开始登录验证：手机号=" + phone, WIDGET_LOG_INFO);

    QJsonObject result;
    QSqlDatabase db = getDatabase();
    if (!db.isOpen()) {
        writeLog("登录验证失败：数据库连接未打开，手机号=" + phone, WIDGET_LOG_ERROR);
        result["success"] = false;
        result["error"] = "数据库连接失败，无法进行登录验证";
        sendResponse(socket, result, 500);
        return;
    }

    // 开启数据库事务，确保查询和更新的原子性
    db.transaction();

    QSqlQuery query(db);
    // 1. 查询用户信息（包含online状态）
    query.prepare("SELECT password, name, online FROM user WHERE phone = :phone");
    query.bindValue(":phone", phone);

    if (!query.exec()) {
        db.rollback(); // 执行失败回滚事务
        writeLog("登录验证失败：查询执行错误，手机号=" + phone + "，错误信息=" + query.lastError().text(), WIDGET_LOG_ERROR);
        result["success"] = false;
        result["error"] = QString("执行登录查询失败: %1").arg(query.lastError().text());
        sendResponse(socket, result, 500);
        return;
    }

    if (query.next()) {
        // 2. 提取用户数据
        QString storedPwd = query.value(0).toString();
        QString userName = query.value(1).toString();
        int onlineStatus = query.value(2).toInt(); // 获取当前在线状态（1=已登录，0=未登录）

        // 3. 检查账号是否已登录
        if (onlineStatus == 1) {
            db.rollback();
            writeLog("登录失败：账号已在登录，手机号=" + phone, WIDGET_LOG_WARNING);
            result["success"] = false;
            result["error"] = "该账号已登录";
            sendResponse(socket, result, 200);
            return;
        }

        // 4. 验证密码
        QString encryptedInputPwd = encryptPassword(password);
        if (encryptedInputPwd == storedPwd) {
            // 5. 密码正确，更新online状态为1（登录中）
            query.prepare("UPDATE user SET online = 1 WHERE phone = :phone");
            query.bindValue(":phone", phone);
            if (!query.exec()) {
                db.rollback();
                writeLog("登录失败：更新在线状态失败，手机号=" + phone + "，错误=" + query.lastError().text(), WIDGET_LOG_ERROR);
                result["success"] = false;
                result["error"] = "登录失败，请重试";
                sendResponse(socket, result, 500);
                return;
            }

            // 提交事务（查询和更新均成功）
            db.commit();
            QString userId=getUserIdByPhone(phone);
            writeLog("登录成功：手机号=" + phone + "，用户名=" + userName + "，ID："+userId, WIDGET_LOG_INFO);
            result["success"] = true;
            result["message"] = "登录成功";
            result["name"] = userName;
            result["phone"] = phone;
            result["userId"] = getUserIdByPhone(phone); // 获取用户ID
            sendResponse(socket, result, 200);
        } else {
            db.rollback();
            writeLog("登录失败：密码错误，手机号=" + phone, WIDGET_LOG_WARNING);
            result["success"] = false;
            result["error"] = "密码错误";
            sendResponse(socket, result, 200);
        }
    } else {
        db.rollback();
        writeLog("登录失败：手机号未注册，手机号=" + phone, WIDGET_LOG_WARNING);
        result["success"] = false;
        result["error"] = "该手机号未注册";
        sendResponse(socket, result, 200);
    }
}

// 发送响应：将JSON结果封装为HTTP响应并发送给客户端
void Widget::sendResponse(QTcpSocket *socket, const QJsonObject &result, int statusCode)
{
    // 将JSON对象转换为字节数组
    QByteArray responseData = QJsonDocument(result).toJson();

    // 构造HTTP响应（使用QByteArray优化拼接效率）
    QByteArray response;
    response.append("HTTP/1.1 ").append(QByteArray::number(statusCode)).append("\r\n");
    response.append("Content-Type: application/json\r\n");
    response.append("Access-Control-Allow-Origin: *\r\n");
    response.append("Content-Length: ").append(QByteArray::number(responseData.size())).append("\r\n\r\n");
    response.append(responseData);

    // 发送响应并清理连接
    socket->write(response);
    socket->flush();
    socket->disconnectFromHost();
    writeLog("响应已发送，状态码=" + QString::number(statusCode), WIDGET_LOG_INFO);
}

// 获取数据库连接：复用全局连接池
QSqlDatabase Widget::getDatabase()
{
    QMutexLocker locker(&dbMutex); // 线程安全

    // 检查连接是否已存在且有效
    if (dbPool.isValid() && dbPool.isOpen()) {
        // 检查连接是否正常（防止意外断开）
        QSqlQuery testQuery("SELECT 1", dbPool);
        if (testQuery.exec()) {
            return dbPool;
        } else {
            writeLog("数据库连接失效，重新连接", WIDGET_LOG_WARNING);
            dbPool.close();
        }
    }

    // 重新连接数据库
    if (!dbPool.open()) {
        writeLog("数据库连接失败：" + dbPool.lastError().text(), WIDGET_LOG_ERROR);
        qCritical () << "数据库连接失败:" << dbPool.lastError ().text ();
    } else {
        writeLog("数据库连接成功（连接池模式）", WIDGET_LOG_INFO);
    }
    return dbPool;
}

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
    writeLog("检查手机号是否已注册：" + phone, WIDGET_LOG_INFO);

    QSqlDatabase db = getDatabase ();
    if (!db.isOpen ()) {
        writeLog("检查手机号失败：数据库未打开", WIDGET_LOG_ERROR);
        return false;
    }

    // 使用预处理语句防止SQL注入
    QSqlQuery query(db);
    query.prepare("SELECT COUNT(*) FROM user WHERE phone = :phone");
    query.bindValue(":phone", phone);

    // 执行查询
    if (!query.exec() || !query.next ()) {
        writeLog("查询手机号失败：" + query.lastError().text(), WIDGET_LOG_WARNING);
        qWarning () << "查询手机号失败:" << query.lastError ().text();
        return false;
    }

    // 返回是否存在（计数>0则存在）
    bool exists = query.value(0).toInt() > 0;
    writeLog("手机号" + phone + (exists ? "已注册" : "未注册"), WIDGET_LOG_INFO);
    return exists;
}

// 生成用户ID：基于当前用户数量生成4位数字ID（不足补0）
QString Widget::generateUserId()
{
    writeLog("开始生成用户ID", WIDGET_LOG_INFO);

    QSqlDatabase db = getDatabase ();
    if (!db.isOpen ()) {
        writeLog("生成用户ID失败：数据库未打开", WIDGET_LOG_ERROR);
        qWarning() << "generateUserId: 数据库未打开";
        return "";
    }

    QSqlQuery query(db);
    if (!query.exec("SELECT COUNT(*) FROM user")) {
        writeLog("生成用户ID失败：查询用户数量错误 - " + query.lastError().text(), WIDGET_LOG_ERROR);
        return "";
    }

    int count = 0;
    if (query.next()) {
        count = query.value(0).toInt();
    }

    // 生成4位数字ID（从0000开始递增）
    QString userId = QString("%1").arg(count+1, 4, 10, QLatin1Char('0'));
    writeLog("生成用户ID成功：" + userId, WIDGET_LOG_INFO);
    return userId;
}

// 注册用户：将用户信息插入数据库
bool Widget::registerUser(const QString &id, const QString &name, const QString &password, const QString &phone,const int online)
{

    writeLog("开始注册用户：ID=" + id + ", 手机号=" + phone, WIDGET_LOG_INFO);

    QSqlDatabase db = getDatabase();
    if (!db.isOpen()) {
        writeLog("注册用户失败：数据库未打开", WIDGET_LOG_ERROR);
        return false;
    }

    QSqlQuery query(db);
    query.prepare("INSERT INTO user (id, name, password, phone,online) VALUES (:id, :name, :password, :phone,:online)");
    query.bindValue(":id", id);
    query.bindValue(":name", name);
    query.bindValue(":password", password);
    query.bindValue(":phone", phone);
    query.bindValue(":online", online);

    if (!query.exec()) {
        writeLog("注册用户失败：" + query.lastError().text(), WIDGET_LOG_ERROR);
        return false;
    }

    return true;
}
// 辅助函数：通过手机号查询用户ID
QString Widget::getUserIdByPhone(const QString& phone) {
    QMutexLocker locker(&dbMutex);
    QSqlQuery query(dbPool);
    query.prepare("SELECT id FROM user WHERE phone = ?");
    query.addBindValue(phone);

    if (query.exec() && query.next()) {
        return query.value(0).toString();
    }
    return ""; // 未找到用户
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

    // 3. 检查是否已成为好友
    QMutexLocker locker(&dbMutex);
    QSqlQuery query(dbPool);
    query.prepare("SELECT id FROM friend WHERE (user_id = ? AND friend_id = ?)");
    query.addBindValue(fromUserId);
    query.addBindValue(toUserId);
    if (query.exec() && query.next()) {
        result["success"] = false;
        result["message"] = "对方已是你的好友";
        return result;
    }

    // 4. 检查是否有未处理的请求
    query.prepare("SELECT id FROM friend_request WHERE from_user_id = ? AND to_user_id = ? AND status = 0");
    query.addBindValue(fromUserId);
    query.addBindValue(toUserId);
    if (query.exec() && query.next()) {
        result["success"] = false;
        result["message"] = "已发送好友请求，请等待回复";
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
        writeLog("发送好友请求失败：" + query.lastError().text(), WIDGET_LOG_ERROR);
        result["success"] = false;
        result["message"] = "发送请求失败，请重试";
    }

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

    QMutexLocker locker(&dbMutex);
    QSqlQuery query(dbPool);

    // 1. 查询请求是否存在
    query.prepare("SELECT from_user_id, to_user_id FROM friend_request WHERE id = ? AND status = 0");
    query.addBindValue(reqId);
    if (!query.exec() || !query.next()) {
        result["success"] = false;
        result["message"] = "好友请求不存在或已处理";
        return result;
    }

    QString fromUserId = query.value(0).toString();
    QString toUserId = query.value(1).toString();

    // 2. 更新请求状态
    query.prepare("UPDATE friend_request SET status = ? WHERE id = ?");
    query.addBindValue(status);
    query.addBindValue(reqId);
    if (!query.exec()) {
        writeLog("更新好友请求状态失败：" + query.lastError().text(), WIDGET_LOG_ERROR);
        result["success"] = false;
        result["message"] = "处理请求失败";
        return result;
    }

    // 3. 如果同意，添加双向好友关系
    if (status == 1) {
        // 插入正向关系
        query.prepare("INSERT INTO friend (user_id, friend_id) VALUES (?, ?)");
        query.addBindValue(toUserId);
        query.addBindValue(fromUserId);
        if (!query.exec()) {
            writeLog("添加好友关系失败：" + query.lastError().text(), WIDGET_LOG_ERROR);
            result["success"] = false;
            result["message"] = "添加好友失败";
            return result;
        }

        // 插入反向关系（便于双向查询）
        query.prepare("INSERT INTO friend (user_id, friend_id) VALUES (?, ?)");
        query.addBindValue(fromUserId);
        query.addBindValue(toUserId);
        query.exec(); // 允许轻微失败，不影响主流程
    }

    result["success"] = true;
    result["message"] = status == 1 ? "已同意好友请求" : "已拒绝好友请求";
    return result;
}

// 获取用户的好友列表（包含昵称）
QJsonObject Widget::getFriendList(const QString& userId) {
    QJsonObject result;
    QJsonArray friendArray;

    QMutexLocker locker(&dbMutex);
    QSqlQuery query(dbPool);
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
        writeLog("查询好友列表失败：" + query.lastError().text(), WIDGET_LOG_ERROR);
        result["success"] = false;
        result["message"] = "获取好友列表失败";
    }

    return result;
}
