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
#include <QSet>
#include <QThreadPool>
#include <QMutex>
#include "connectionpool.h"

QT_BEGIN_NAMESPACE
namespace Ui { class Widget; }
QT_END_NAMESPACE

class Widget : public QWidget
{
    Q_OBJECT

public:
    Widget(QWidget *parent = nullptr);
    ~Widget();

signals:

private:
    Ui::Widget *ui;                  // UI界面对象
    QTcpServer *server;              // TCP服务器，负责监听客户端连接
    QString m_lastMessageId;  // 最后一条消息ID
    QString m_myUserId;       // 当前登录用户ID
    QMap<QString, QTcpSocket*> m_onlineClients; // 在线用户 ID 与 TCP Socket 的映射
    QSet<QTcpSocket*> m_pendingSockets; // 待处理协议识别的 Socket 集合
    QMap<QTcpSocket*, QByteArray> m_tcpBuffers; // 为每个 TCP 连接维护独立缓冲区
    QThreadPool *m_threadPool;         // 线程池
    QMutex m_onlineClientsMutex;       // 保护 m_onlineClients 的互斥锁

    // 核心业务处理函数
    void handleIncomingConnection();                  // 处理新连接
    void handleProtocolDetection(QTcpSocket *socket);  // 协议识别
    void handleData(QTcpSocket *socket);              // 处理 TCP 数据流
    void processPacket(QTcpSocket *socket, const QJsonObject &json); // 分发业务逻辑
    bool registerUser(const QString &id, const QString &name, const QString &password,
                      const QString &phone, const int online); // 用户注册
    void check_login(QString phone, QString password, QTcpSocket* socket); // 登录验证
    void sendPacket(QTcpSocket *socket, const QJsonObject &json); // 发送 TCP 数据包
    QString encryptPassword(const QString &password); // 密码加密
    bool isPhoneExists(const QString &phone);         // 检查手机号是否已注册
    QString generateUserId();                         // 生成唯一用户ID
    QString getUserIdByPhone(const QString& phone);   // 通过手机号查询用户ID

    // 好友功能相关
    QJsonObject sendFriendRequestByPhone(const QString& fromUserId, const QString& toPhone); // 发送好友请求
    QJsonObject handleFriendRequest(const QString& reqId, int status);                       // 处理好友请求
    QJsonObject getFriendList(const QString& userId);                                       // 获取好友列表
    // 新增消息相关处理函数
    void handleSendMessage(const QJsonObject& requestData, QTcpSocket* socket); // 处理发送消息请求 (TCP)
    QString saveMessage(const QString& fromUserId, const QString& toUserId, const QString& content); // 保存消息
    QList<QJsonObject> queryMessages(const QString &m_myUserId, const QString &toUserId, const QString &m_lastMessageId);
    bool updateAvatarPath(const QString &userId, const QString &path);
    void handleAvatarUpload(const QJsonObject &json, QTcpSocket *socket);


    // 数据库管理转移至 ConnectionPool


};

#endif // WIDGET_H
