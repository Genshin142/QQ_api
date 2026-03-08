#ifndef CONNECTIONPOOL_H
#define CONNECTIONPOOL_H

#include <QSqlDatabase>
#include <QQueue>
#include <QMutex>
#include <QMutexLocker>
#include <QWaitCondition>
#include <QDebug>

class ConnectionPool
{
public:
    // 获取单例实例
    static ConnectionPool& instance();

    // 从连接池获取一个连接
    static QSqlDatabase openConnection();

    // 将连接归还给连接池
    static void closeConnection(const QSqlDatabase &db);

    // 释放所有连接资源（程序退出时调用）
    static void release();

private:
    ConnectionPool();
    ~ConnectionPool();

    // 禁用拷贝和赋值
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    // 创建一个新的命名连接
    QSqlDatabase createConnection(const QString &connectionName);

    // 数据库配置信息
    QString m_hostName;
    QString m_databaseName;
    QString m_username;
    QString m_password;
    int m_port;

    int m_maxConnections;              
    QQueue<QString> m_unusedNames;    // 保持作为备份，虽然主要使用线程本地
    QList<QString> m_allNames;        // 追踪所有创建的连接以便释放

    static QMutex m_mutex;             // 保护 m_allNames
};

#endif // CONNECTIONPOOL_H
