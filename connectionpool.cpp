#include "connectionpool.h"
#include <QSqlError>
#include <QThreadStorage>
#include <QThread>

// 静态成员初始化
QMutex ConnectionPool::m_mutex;

static QThreadStorage<QString> s_threadConnectionName;

ConnectionPool::ConnectionPool()
    : m_hostName("localhost")
    , m_databaseName("load_data")
    , m_username("root")
    , m_password("362345943")
    , m_port(3306)
    , m_maxConnections(20) 
{
}

ConnectionPool::~ConnectionPool()
{
}

ConnectionPool& ConnectionPool::instance()
{
    static ConnectionPool pool;
    return pool;
}

QSqlDatabase ConnectionPool::openConnection()
{
    ConnectionPool &pool = instance();
    QMutexLocker locker(&m_mutex); // 核心修改：在整个过程中保持锁定
    
    if (!s_threadConnectionName.hasLocalData()) {
        QString name = QString("PoolConn_0x%1").arg(QString::number((quintptr)QThread::currentThread(), 16));
        s_threadConnectionName.setLocalData(name);
        qDebug() << "为新线程分配数据库连接名:" << name;
    }

    QString name = s_threadConnectionName.localData();
    
    if (QSqlDatabase::contains(name)) {
        QSqlDatabase db = QSqlDatabase::database(name);
        if (!db.isOpen()) {
            if (!db.open()) {
                qWarning() << "无法在当前线程重新打开连接:" << name << db.lastError().text();
            }
        }
        return db;
    }

    // 创建新连接（此时仍持有锁，addDatabase 安全）
    QSqlDatabase db = pool.createConnection(name);
    if (db.isOpen()) {
        pool.m_allNames.append(name);
    }
    return db;
}

void ConnectionPool::closeConnection(const QSqlDatabase &db)
{
    // 在线程本地模型中，连接通常长期保留在线程中，直到线程销毁或 release() 被调用。
    // 这里保持为空，或者仅做简单验证。
    Q_UNUSED(db);
}

void ConnectionPool::release()
{
    ConnectionPool &pool = instance();
    QMutexLocker locker(&m_mutex);

    for (const QString &name : pool.m_allNames) {
        if (QSqlDatabase::contains(name)) {
            {
                // 先关闭再移除
                QSqlDatabase db = QSqlDatabase::database(name, false);
                if (db.isOpen()) db.close();
            }
            QSqlDatabase::removeDatabase(name);
        }
    }
    pool.m_allNames.clear();
}

QSqlDatabase ConnectionPool::createConnection(const QString &connectionName)
{
    QSqlDatabase db = QSqlDatabase::addDatabase("QMYSQL", connectionName);
    db.setHostName(m_hostName);
    db.setDatabaseName(m_databaseName);
    db.setUserName(m_username);
    db.setPassword(m_password);
    db.setPort(m_port);

    if (!db.open()) {
        qCritical() << "ConnectionPool failed to open database:" << db.lastError().text() << " (Connection: " << connectionName << ")";
    }
    return db;
}
