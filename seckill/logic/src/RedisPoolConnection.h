/*
* ==============================================
*        Filename: RedisPoolConnectioin.h
*        Description: 
* ==============================================
*/
#ifndef _REDISPOOLCONNECTIOIN_H_
#define _REDISPOOLCONNECTIOIN_H_

#include<iostream>
#include<queue>
#include<map>
#include<vector>
#include<utility>
#include<string>
#include<mutex>
#include<thread>
#include <hiredis/hiredis.h>
#include<string.h>

using namespace std;


class RedisPool
{
    public:
        
        virtual ~RedisPool();
        static   RedisPool * GetInstance();  
        int InitPool(const string& ip, int port, int maxconn,int timeout);

        bool ExecuteCmd(redisContext* pRedis, 
                         const char *cmd,
                         size_t len,
                         string &response);

        redisReply* ExecuteCmd(redisContext* pRedis, const char *cmd, size_t len);
        redisContext* GetConnection();
        void CloseConnection(redisContext *ctx);

    private:
        RedisPool();
        redisContext* CreateConnectiont();
        bool CheckStatus(redisContext *ctx);

    private:
        std::queue<redisContext *> m_clients;
        string m_setverIp;
        int m_timeout;
        int m_serverPort;
	int m_connectCount;
        
        time_t m_beginInvalidTime;
        static const int m_maxReconnectInterval = 3;
        static std::mutex m_geneLock;                   
        static std::mutex m_pollLock;                 
        static RedisPool* m_pRedisPool;  
};

#endif
