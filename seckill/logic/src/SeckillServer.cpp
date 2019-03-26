/*
 * ==============================================
 *        Filename: SeckillServer.cpp
 *        Description: 
 * ==============================================
 */
#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "SeckillWork.h"

const std::string g_strServerAddr = "0.0.0.0:50051";
const int g_iReceSize = 3000;
const int g_iSendSize = 3000;
const int g_iWorkerNum = 30;

const std::string g_strMysqlAddr     = "192.168.59.133";
const std::string g_strMysqlUserName = "root";
const int         g_iMysqlPort       = 3306; 
const std::string g_strUserPassword  = "root_password";
const std::string g_strMysqlDB       = "seckill";
const int         g_iMaxConnectCount = 30;

const std::string g_strRedisAddr = "192.168.59.133";
const int         g_iRedisPort   =  6379;
const int         g_iRedisMaxCount = 30;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using seckill::SeckillRequest;
using seckill::SeckillResponse;
using seckill::SeckillService;


class ServerImpl final{

    public:
        ~ServerImpl() {
            m_oServer->Shutdown();  
            m_oCqueue->Shutdown();
        }

        void Run(MysqlPool *pMysqlPool,RedisPool *pRedisPool) {

            m_iFailNum = 0;
            m_iSucNum = 0; 
            pthread_rwlock_init(&m_rwlock, NULL);
            m_pRedisPool = pRedisPool;

            std::string strServerAddress(g_strServerAddr);
            ServerBuilder builder;
            builder.AddListeningPort(strServerAddress, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_oService);
            builder.SetMaxReceiveMessageSize(g_iReceSize);
            builder.SetMaxSendMessageSize(g_iSendSize);

            m_oCqueue = builder.AddCompletionQueue();
            m_oServer = builder.BuildAndStart();

            std::cout << "Server listening on " <<g_strServerAddr << std::endl;

            new SeckillWork(&m_oService, 
                    m_oCqueue.get(),
                    pMysqlPool,
                    pRedisPool, 
                    &m_rwlock,
                    &m_iFailNum,
                    &m_iSucNum);

            pthread_t tids[g_iWorkerNum ];
            int iRet = 0;
            for ( int i = 0 ; i < g_iWorkerNum ; i++ ){
                iRet  = pthread_create(&tids[i],
                         NULL, 
                         ServerImpl::ThreadHandler,
                         (void*)this);
                if (iRet != 0) {
			std::cout << "pthread_create.failed=" << iRet << std::endl;
		}else {
		}
	    }

            for ( int i = 0 ; i < g_iWorkerNum ; i++ ) {
                pthread_join(tids[0],NULL);
            }
        }

    private:

        static void * ThreadHandler(void* lparam) {
		ServerImpl* impl = (ServerImpl*)lparam;
		impl->HandleReq();
		return ((void *)0);
	}

        //thread function
        void HandleReq() {
            void* pTag = NULL;; 
            bool bOk = false;;

            while (true) {
                //wait request
		GPR_ASSERT(m_oCqueue->Next(&pTag, &bOk));
                GPR_ASSERT(bOk);
                static_cast<SeckillWork*>(pTag)->Proceed();
            }

        }

    private:
        int m_iSucNum ;
        int m_iFailNum;
        pthread_rwlock_t m_rwlock;
        RedisPool *m_pRedisPool;
        std::shared_ptr<ServerCompletionQueue> m_oCqueue;
        SeckillService::AsyncService m_oService;
        std::shared_ptr<Server> m_oServer;
};

int GeneData(MysqlPool *pMysql, RedisPool *pRedis)
{
    MYSQL *pMysqlConn         =  pMysql->GetConnection();
    redisContext * pRedisConn =  pRedis->GetConnection();
    int iRet = 0;

    if(NULL == pMysqlConn || NULL == pRedisConn) {
        return -__LINE__;
    }

    std::string strMysqlCmd;

    //mysql user_info
    strMysqlCmd = "DROP TABLE IF EXISTS user_info";   
    iRet = mysql_query(pMysqlConn,strMysqlCmd.c_str());
    if(iRet != 0) {
        return -__LINE__;
    }

    strMysqlCmd = "CREATE TABLE user_info(user_name VARCHAR(100) NOT NULL,user_key VARCHAR(100) NOT NULL,PRIMARY KEY (user_name))" ;
    iRet = mysql_query(pMysqlConn,strMysqlCmd.c_str());
    if(iRet != 0) {
	    return -__LINE__;
    }

    std::string insertSql("INSERT INTO user_info(user_name,user_key) VALUES ");
    for (int i = 1; i <=400; i ++) {
        std::string strUserKey = std::to_string (i) + std::to_string (i) + SALT;
        MD5 iMD5(strUserKey);
        std::string md5_str(iMD5.toStr());
        if (400 == i) {
            insertSql =  insertSql + "('" + std::to_string (i) + "','" + md5_str + "')";
        } else{
            insertSql =  insertSql + "('" + std::to_string (i) + "','" + md5_str + "'),";
        }
    }
    iRet = mysql_query(pMysqlConn,insertSql.c_str());
    if(iRet != 0) {
	    return -__LINE__;
    }

    //mysql goods_info
    strMysqlCmd = "DROP TABLE IF EXISTS goods_info";   
    iRet = mysql_query(pMysqlConn,strMysqlCmd.c_str());
    if(iRet != 0) {
        return -__LINE__;
    }

    strMysqlCmd = "CREATE TABLE IF NOT EXISTS goods_info(goods_name VARCHAR(20) NOT NULL,goods_id VARCHAR(20) NOT NULL,goods_total_count INT UNSIGNED NOT NULL,PRIMARY KEY (goods_id))"; 
    iRet = mysql_query(pMysqlConn, strMysqlCmd.c_str());
    if(iRet != 0) {
        return -__LINE__;
    }
    strMysqlCmd  = "INSERT INTO goods_info(goods_name, goods_id, goods_total_count) VALUES('GOODS', '1', 50) ON DUPLICATE KEY UPDATE goods_id = '1', goods_total_count = 50";
    iRet = mysql_query(pMysqlConn, strMysqlCmd.c_str());
    if(iRet != 0) {
        return -__LINE__;
    }
   
    //mysql order_info
    strMysqlCmd = "DROP TABLE IF EXISTS  order_info";   
    iRet = mysql_query(pMysqlConn, strMysqlCmd.c_str());
    if(iRet != 0) {
        return -__LINE__;
    }
    strMysqlCmd = "CREATE TABLE IF NOT EXISTS order_info(user_name VARCHAR(100) NOT NULL,user_password VARCHAR(100) NOT NULL,goods_id VARCHAR(100) NOT NULL,PRIMARY KEY (user_name));";
    iRet = mysql_query(pMysqlConn, strMysqlCmd.c_str());
    if(iRet != 0) {
	    return -__LINE__;
    }

    //redis data clear
    redisCommand(pRedisConn, "flushall");

    //redis user info
    std::string searchSql("SELECT * FROM user_info");
    iRet = mysql_query(pMysqlConn, searchSql.c_str());
    if(iRet != 0) {
	    std::cout << "Query Usr_Info Error:" << mysql_error(pMysqlConn);
	    return -__LINE__;
    }

    MYSQL_RES *searchResult = mysql_use_result(pMysqlConn);
    MYSQL_ROW searchrow;
    redisReply *reply = NULL;
    while((searchrow = mysql_fetch_row(searchResult)) != NULL) {
	    if (mysql_num_fields(searchResult) > 1) {
		    std::string name(searchrow[0]);
		    std::string key(searchrow[1]);

		    reply = (redisReply *)redisCommand(pRedisConn, "HMSET user_info  %s %s", name.c_str(), key.c_str());

		    if(reply != NULL && reply->type == REDIS_REPLY_STATUS && (strcasecmp(reply->str,"OK") == 0)) {
			    freeReplyObject(reply);
		    }else {
			    std::cout << "redis set count error ";
			    if (reply != NULL ) {
				    std::cout << "error message:" << pRedisConn->errstr << std::endl;
				    freeReplyObject(reply);
			    }
			    redisFree(pRedisConn);
			    break;
		    }
	    }
    }//while;
    mysql_free_result(searchResult);


    //reids total_count
    int iTotalCount = 0;
    strMysqlCmd =  "SELECT * FROM goods_info";
    iRet = mysql_query(pMysqlConn, strMysqlCmd.c_str());
    if(iRet != 0) {
	    std::cout << "Query GoodsCount Error:" << mysql_error(pMysqlConn);
	    return -__LINE__;
    }
    MYSQL_RES *result = mysql_use_result(pMysqlConn); 
    MYSQL_ROW row;
    while((row = mysql_fetch_row(result)) != NULL) {
	    if (mysql_num_fields(result) > 1){ 
		    iTotalCount  = atoi(row[2]);
	    }
    }
    mysql_free_result(result);


    std::string strCount = std::to_string(iTotalCount);
    reply = (redisReply *)redisCommand(pRedisConn, "SET %s %s", "total_count",strCount.c_str());
    if(reply != NULL && reply->type == REDIS_REPLY_STATUS && (strcasecmp(reply->str,"OK") == 0))
    {
	    std::cout << "redis set count OK!" << std::endl;
	    freeReplyObject(reply);
    }else {
	    std::cout << "redis set count error ";
	    if (reply != NULL ) {
		    std::cout << "error message:" <<pRedisConn->errstr << std::endl;
		    freeReplyObject(reply);
	    }

	    pMysql->CloseConnection(pMysqlConn);
	    pRedis->CloseConnection(pRedisConn);
	    return -__LINE__;
    }

    //delet order_info 
    //redisCommand(pRedisConn, "DEL %s", "order_info");

    pMysql->CloseConnection(pMysqlConn);
    pRedis->CloseConnection(pRedisConn);

    return 0;
}

int main(int argc, char *argv[])
{
	MysqlPool *pMysqlPool = MysqlPool::GetInstance();
	RedisPool *pRedisPool = RedisPool::GetInstance();
	if(NULL == pMysqlPool  || NULL == pRedisPool) {
		printf("Get pool failed.\n");
		return 0;
	}

	pMysqlPool-> InitPoolConnection(g_strMysqlAddr.c_str(),
			g_strMysqlUserName.c_str(),
			g_strUserPassword.c_str(),
			g_strMysqlDB.c_str(),
			g_iMysqlPort,
			NULL,
			0,
			g_iMaxConnectCount);

	pRedisPool->InitPool(g_strRedisAddr ,
			g_iRedisPort   ,
			500,
			g_iRedisMaxCount);

	int iRet = GeneData(pMysqlPool,pRedisPool);
	if(iRet != 0) {
		std::cout<<iRet<<endl;
		return 0;
	}

	ServerImpl server;
	server.Run(pMysqlPool,pRedisPool);
	return 0;
}

