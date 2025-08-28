#ifndef WIDGET_H
#define WIDGET_H

#include <QWidget>
#include <QTcpServer>
#include <QTcpSocket>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QQueue>
#include <QMutex>
#include <QWaitCondition>
#include <QThread>

QT_BEGIN_NAMESPACE
namespace Ui { class Widget; }
QT_END_NAMESPACE

// 日志级别枚举
enum LogType {
    WIDGET_LOG_INFO,    // 常规操作记录
    WIDGET_LOG_WARNING, // 需要注意的异常
    WIDGET_LOG_ERROR    // 影响功能的错误
};

class Widget : public QWidget
{
    Q_OBJECT

public:
    Widget(QWidget *parent = nullptr);
    ~Widget();

    // 写入日志到文件和控制台
    static void writeLog(const QString& message, LogType type = WIDGET_LOG_INFO);

signals:
    // 预留：向UI线程发送日志更新信号
    void logToUI(const QString& logStr);

private:
    Ui::Widget *ui;                  // UI界面对象
    QTcpServer *server;              // TCP服务器，负责监听客户端连接
    QString m_lastMessageId;  // 最后一条消息的ID，用于增量拉取
    QString m_myUserId;       // 当前登录用户ID

    // 核心业务处理函数
    void handleRequest(QTcpSocket *socket);           // 解析并处理客户端请求
    QSqlDatabase getDatabase();                       // 获取数据库连接（连接池模式）
    bool registerUser(const QString &id, const QString &name, const QString &password,
                      const QString &phone, const int online); // 用户注册
    void check_login(QString phone, QString password, QTcpSocket* socket); // 登录验证
    void sendResponse(QTcpSocket *socket, const QJsonObject &result, int statusCode); // 发送HTTP响应
    QString encryptPassword(const QString &password); // 密码加密（SHA256）
    bool isPhoneExists(const QString &phone);         // 检查手机号是否已注册
    QString generateUserId();                         // 生成唯一用户ID
    QString getUserIdByPhone(const QString& phone);   // 通过手机号查询用户ID

    // 好友功能相关
    QJsonObject sendFriendRequestByPhone(const QString& fromUserId, const QString& toPhone); // 发送好友请求
    QJsonObject handleFriendRequest(const QString& reqId, int status);                       // 处理好友请求（同意/拒绝）
    QJsonObject getFriendList(const QString& userId);                                       // 获取好友列表
    // 新增消息相关处理函数
    void handleSendMessage(const QJsonObject& requestData, QTcpSocket* socket); // 处理发送消息请求
    bool saveMessage(const QString& fromUserId, const QString& toUserId, const QString& content); // 保存消息到数据库
    QList<QJsonObject> queryMessages(const QString &m_myUserId, const QString &toUserId, const QString &m_lastMessageId);
    bool updateAvatarPath(const QString &userId, const QString &path);
    void handleAvatarUpload(const QJsonObject &json, QTcpSocket *socket);


    // 数据库连接池（静态成员保证全局唯一）
    static QSqlDatabase dbPool;
    static QMutex dbMutex;         // 数据库操作互斥锁（线程安全）

    // 异步日志系统（静态成员支持跨线程日志处理）
    static QQueue<QString> logQueue;    // 日志队列
    static QMutex logMutex;             // 日志队列互斥锁
    static QWaitCondition logCond;      // 日志线程唤醒条件
    static bool logRunning;             // 日志线程运行标志
    static void logWorker();            // 日志写入工作线程
};

#endif // WIDGET_H
