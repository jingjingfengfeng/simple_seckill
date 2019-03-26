/*
 * ==============================================
 *        Filename: RedisPoolConnectioin.cpp
 *        Description: 
 * ==============================================
 */
#include "RedisPoolConnection.h"

RedisPool* RedisPool::m_pRedisPool = NULL;
std::mutex RedisPool::m_pollLock;
std::mutex RedisPool::m_geneLock;

RedisPool::RedisPool() {}

RedisPool::~RedisPool()
{
    while(!m_clients.empty())
    {
        redisContext *ctx = m_clients.front();
        redisFree(ctx);
        m_clients.pop();
	m_connectCount--;
    }
}

int RedisPool::InitPool(const string & ip, int port, int maxconn, int timeout)
{
    m_timeout = timeout;
    m_serverPort = port;
    m_setverIp = ip;
    m_beginInvalidTime = 0;
    m_connectCount = 0;
}

RedisPool* RedisPool::GetInstance() 
{
    if ( NULL == m_pRedisPool) { 
        m_geneLock.lock();

        if (m_pRedisPool == NULL) {
            m_pRedisPool = new RedisPool();
        }

        m_geneLock.unlock();
    }
    return  m_pRedisPool;
}

redisContext* RedisPool::CreateConnectiont()
{
    time_t now = time(NULL);
    if(now < m_beginInvalidTime + m_maxReconnectInterval) {
        return NULL;
    }

    struct timeval tv;
    tv.tv_sec = m_timeout / 1000;
    tv.tv_usec = (m_timeout % 1000) * 1000;;
    redisContext *ctx = redisConnectWithTimeout(m_setverIp.c_str(), m_serverPort, tv);
    if(ctx == NULL || ctx->err != 0) {
        if(ctx != NULL) redisFree(ctx);
        m_beginInvalidTime = time(NULL);
        return NULL;
    }
    return ctx;
}

redisContext* RedisPool::GetConnection()
{
   m_pollLock.lock();
   if(!m_clients.empty()) {
	redisContext *ctx = m_clients.front();
	m_clients.pop();
	m_pollLock.unlock();
	return ctx;
    }
    m_pollLock.unlock();

    time_t now = time(NULL);
    if(now < m_beginInvalidTime + m_maxReconnectInterval){
	    return NULL;
    }

    struct timeval tv;
    tv.tv_sec = m_timeout / 1000;
    tv.tv_usec = (m_timeout % 1000) * 1000;;
    redisContext *ctx = redisConnectWithTimeout(m_setverIp.c_str(), m_serverPort, tv);
    if(ctx == NULL || ctx->err != 0) {
        if(ctx != NULL) redisFree(ctx);

        m_beginInvalidTime = time(NULL);
        return NULL;
    }

    return ctx;
}

void RedisPool::CloseConnection(redisContext *ctx)
{
	if(ctx == NULL) return;

	m_pollLock.lock();
	m_clients.push(ctx);
	m_pollLock.unlock();
}

bool RedisPool::CheckStatus(redisContext *ctx)
{

    redisReply *reply = (redisReply*)redisCommand(ctx, "ping");
    if(reply == NULL) return false;

    if(reply->type != REDIS_REPLY_STATUS){ 
	    freeReplyObject(reply);
	    return false;
    }

    if(strcasecmp(reply->str,"PONG") != 0) {
	    freeReplyObject(reply);
	    return false;
    }

    return true;
}
