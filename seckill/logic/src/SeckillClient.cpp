#include <iostream>
#include <string>
#include <pthread.h>

#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include <grpc/support/log.h>

#include "seckill.grpc.pb.h"
#include "MysqlPoolConnection.h"
#include "Md5.h"

const std::string g_strMysqlAddr     = "127.0.0.1";
const std::string g_strMysqlUserName = "seckill_user";
const int         g_iMysqlPort       = 3306; 
const std::string g_strUserPassword  = "seckill_password";
const std::string g_strMysqlDB       = "seckill";
const int         g_iMaxConnectCount = 30;

#define Random(x) (rand() % x)
#define SALT "salt"

int g_iFailCount = 0;
int g_iSucCount = 0;
std::mutex g_mtx;

std::mutex mtx_g_iAllReq;;
int g_iAllReq = 0;

std::mutex mtx_g_iAllRsp;;
int g_iAllRsp = 0;

const int g_iClientNum = 500;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using seckill::SeckillRequest;
using seckill::SeckillResponse;
using seckill::SeckillService;
using namespace std;

class SeckillClient 
{
    public:
        explicit SeckillClient(std::shared_ptr<Channel> channel)
            : m_stub(SeckillService::NewStub(channel)) {}

        int  Seckill(const std::string &strUserName,const std::string &strUserKey) 
        {
            SeckillRequest stReq;
            stReq.set_str_user_name(strUserName);
            stReq.set_str_user_key(strUserKey);

            SeckillResponse stRsp;
            ClientContext stContext;
            CompletionQueue stCq;

            Status status;

            std::cout<<stReq.DebugString().c_str()<<endl;
            std::unique_ptr<ClientAsyncResponseReader<SeckillResponse>> rpc(m_stub->PrepareAsyncseckill(&stContext, stReq, &stCq));

            rpc->StartCall();
            rpc->Finish(&stRsp, &status, (void*)1);
            void* got_tag;
            bool ok = false;

            GPR_ASSERT(stCq.Next(&got_tag, &ok));
            std::cout<<stRsp.DebugString().c_str()<<endl;

            GPR_ASSERT(got_tag == (void*)1);
            GPR_ASSERT(ok);

            if (status.ok()){
                return stRsp.uint32_result();
            } else {
                return 0;
            }
        }
    private:
        std::unique_ptr<SeckillService::Stub> m_stub;
};

int CheckSeckillResult()
{
    bool isnormol = true;
    MysqlPool *pMysqlPool = MysqlPool::GetInstance();
    pMysqlPool->InitPoolConnection(g_strMysqlAddr.c_str(),
		    g_strMysqlUserName.c_str(),
		    g_strUserPassword.c_str(),
		    g_strMysqlDB.c_str(),
		    g_iMysqlPort,
		    NULL,
		    0,
		    g_iMaxConnectCount);
    MYSQL *pMysqlConn= pMysqlPool->GetConnection();
    if(NULL == pMysqlConn){
        return -__LINE__;
    }

    if (g_iSucCount > 50){
        std::cout << "seckill too much" << std::endl;
        return -__LINE__;
    }

    int iTotalCount = 0;
    int iRet = 0;
    std::string selSql("SELECT goods_total_count FROM goods_info WHERE goods_id ='1' ");
    iRet = mysql_query(pMysqlConn, selSql.c_str());
    if(iRet != 0) {
        std::cout << "mysql_query failed." << mysql_error(pMysqlConn);
        return -__LINE__;
    }

    MYSQL_RES *pResult = mysql_use_result(pMysqlConn);
    MYSQL_ROW row;
    while((row = mysql_fetch_row(pResult)) != NULL) {
        if (mysql_num_fields(pResult) > 0){
            iTotalCount = atoi(row[0]);
        }
    }
    if (pResult!= NULL) {
        mysql_free_result(pResult);
    }

    if (g_iSucCount + iTotalCount != 50) {
        return -__LINE__;
    }

    char sSql[200] = {0};
    sprintf(sSql,"SELECT user_name FROM order_info");
    iRet = mysql_query(pMysqlConn,sSql); 
    if(iRet != 0) {
        std::cout<<" mysql_query failed"<<endl;
        return -__LINE__;
    }
    MYSQL_RES *pRes = mysql_store_result(pMysqlConn);
    if(NULL == pRes) {
        return -__LINE__;
    }

    map<string, int> mapUserName;
    while ((row = mysql_fetch_row(pRes)) != NULL) {
        if(mysql_num_fields(pRes) > 0) {
            mapUserName[row[0]] ++;
            if(mapUserName[row[0]] != 1) {
                std::cout<<"dup order"<<endl;
            }
        }
    }
    if( mapUserName.size() != g_iSucCount) {
        std::cout<<"diff num" <<endl;
        return -__LINE__;
    }

    mysql_free_result(pRes);
    pMysqlPool->CloseConnection(pMysqlConn);
    return 0;
}

void* SeckillProc(void* args)
{
    int iUser = *(int *)args;
    std::string strUser = std::to_string (iUser) + std::to_string (iUser) + SALT;
    MD5 iMD5(strUser);
    std::string strPassWord(iMD5.toStr());
    std::string strName = std::to_string(iUser);

    mtx_g_iAllReq.lock();
    g_iAllReq ++;
    mtx_g_iAllReq.unlock();

    SeckillClient SeckillClient(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
    int  iReply = SeckillClient.Seckill(strName,strPassWord); 

    mtx_g_iAllRsp.lock();
    g_iAllRsp ++;
    mtx_g_iAllRsp.unlock();

    g_mtx.lock();
    if (1 == iReply) {
	    ++ g_iSucCount;
    }else {
	    ++ g_iFailCount;
    }
    g_mtx.unlock();

    
    return ((void *)0);
}

int main(int argc, char** argv) 
{
    int strUser[g_iClientNum] = {0};
    pthread_t tids[g_iClientNum];

    for (int i = 0; i < g_iClientNum; ++i) {
        strUser[i] = Random(600);
    }

    int iRet = 0;
    for(int i = 0; i <  g_iClientNum; ++i) {
        int iRet  = pthread_create(&tids[i], NULL, SeckillProc, &strUser[i]);
        if (iRet != 0) {
            std::cout << "pthread_create.iRet=" << iRet << std::endl;
        }else{
            pthread_join(tids[i],NULL);
        }
    }

    std::cout<<"g_iAllReq="<<g_iAllReq<<endl;
    std::cout<<"g_iAllRsp="<<g_iAllRsp<<endl;
    std::cout<<"g_iSucCount="<< g_iSucCount<<endl;
    std::cout<<"g_iFailCount="<< g_iFailCount<<endl;
    iRet = CheckSeckillResult();

    if (0 == iRet) {
	    std::cout << "seckill num: "<< g_iSucCount << std::endl;
    }else {
	    std::cout << "system error"<<iRet<<" seckill num:"<< g_iSucCount <<  std::endl;
    }

    return 0;
}
