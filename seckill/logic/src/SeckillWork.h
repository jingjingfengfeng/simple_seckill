/*
* ==============================================
*        Author: jolanxiao   Email:jolanxiao@tencent.com
*        Create Time: 2019-03-20 18:59
*        Last modified: 2019-03-20 18:59
*        Filename: SeckillWork.h
*        Description: 
* ==============================================
*/
#ifndef _SECKILLWORK_H_
#define _SECKILLWORK_H_

#include <grpc++/security/server_credentials.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc/grpc.h>
#include <grpc/support/log.h>

#include "seckill.grpc.pb.h"

#include <hiredis/hiredis.h>

#include "Md5.h"
#include "LogApi.h"
#include "MysqlPoolConnection.h"
#include "RedisPoolConnection.h"
// #include <thread>
#include <unistd.h>
#include <pthread.h>

#define SALT "salt"

using namespace std;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using seckill::SeckillRequest;
using seckill::SeckillResponse;
using seckill::SeckillService;
using grpc::ServerAsyncResponseWriter;


class SeckillWork {

    public:
        SeckillWork(SeckillService::AsyncService* pService, 
                ServerCompletionQueue* pCq, 
                MysqlPool *pMysqlPool,
                RedisPool *pRedisPool, 
                pthread_rwlock_t *pRwLock,
                int *pFailedNum,
                int * pSuccessNum) :m_pService(pService), 
                                    m_pCq(pCq),
                                    m_oRspWriter(&m_oCtx),
                                    m_eStatus(CREATE),
                                    m_pMysqlPool(pMysqlPool),
                                    m_pRedisPool(pRedisPool),
                                    m_pRwLock(pRwLock),
                                    m_pFailedNum(pFailedNum),
                                    m_pSuccessNum(pSuccessNum){

                                        Proceed(); //开始处理
                                    }

	void Proceed();
    private:
	int CheckUser(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn,
                MYSQL * pMysqlConn);

        int CheckUserOnRedis(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn);

        int CheckUserOnMysql(const string & strUserName,
                const string &strUserKey,
                MYSQL * pMysqlConn);

        int CheckDup(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn,
                MYSQL * pMysqlConn);

        int CheckDupOnRedis(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn);

        int CheckDupOnMysql(const string & strUserName,
                const string &strUserKey,
                MYSQL * pMysqlConn);

        int SeckillProc(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn,
                MYSQL * pMysqlConn);

        int  SeckillProcOnRedis(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn,
                MYSQL *pMysqlConn,
                int iRetryCount);

        int SeckillProcOnMysql(const string & strUserName,
                const string &strUserKey,
                MYSQL * pMysqlConn);


	int SeckillProcOnMysqlTransaction(const string & strUserName, const string &strUserKey, MYSQL * pMysqlConn);

	int SeckillProcOnRedisTransaction(const string & strUserName, const string &strUserKey, redisContext * pRedisConn, int iRetryCount);


    private:
        enum WorkerStatus  {
            CREATE, 
            PROCESS,
            FINISH };

        SeckillService::AsyncService* m_pService;
        ServerCompletionQueue* m_pCq;
        ServerContext m_oCtx;

        SeckillRequest m_oReq;;
        SeckillResponse m_oRsp;
        ServerAsyncResponseWriter<SeckillResponse> m_oRspWriter;

        WorkerStatus m_eStatus;

        MysqlPool *m_pMysqlPool;
        RedisPool *m_pRedisPool;

        pthread_rwlock_t *m_pRwLock;
        int *m_pFailedNum;
        int *m_pSuccessNum;


};
#endif
