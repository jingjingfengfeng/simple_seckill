/*
* ==============================================
*        Filename: SeckillWork.cpp
*        Description: 
* ==============================================
*/
#include "SeckillWork.h"
const int g_iMaxTry = 10;

void SeckillWork::Proceed() 
{
    if (m_eStatus == CREATE) {
        m_eStatus = PROCESS;
        m_pService->Requestseckill(&m_oCtx, 
			           &m_oReq, 
				   &m_oRspWriter, 
				   m_pCq,
				   m_pCq,
				   this);
	std::cout<<"create work"<<endl;

    } else if (m_eStatus == PROCESS) {
        // cnnect redis
        int iRet = 0;
        redisContext *pRedisConn = m_pRedisPool->GetConnection();
        MYSQL *mysqlpMysqlConn = m_pMysqlPool->GetConnection();
        m_eStatus = FINISH;

	int iWorkingNum = 0;
	redisReply *pReply = (redisReply *)redisCommand(pRedisConn, "llen %s", "work_list");
	if(NULL != pReply &&  REDIS_REPLY_INTEGER == pReply->type){
		iWorkingNum = pReply->integer; 
		freeReplyObject(pReply);
	}


        redisCommand(pRedisConn, "LPUSH work_list %s", "1");
	new SeckillWork(m_pService, 
			m_pCq,
			m_pMysqlPool,
			m_pRedisPool,
			m_pRwLock ,
			m_pFailedNum,
			m_pSuccessNum
		       );

        auto strUserName = m_oReq.str_user_name();
        auto strUserKey = m_oReq.str_user_key();

        std::thread::id tid = std::this_thread::get_id();

	do {
		//check max num
		if(*m_pSuccessNum  >= 50) {
			m_oRsp.set_uint32_result(0);
			m_oRsp.set_str_err_msg("sold out");
			LogWater(LOG_DEBUG,"1 working=%u succeedNum=%u",iWorkingNum,*m_pSuccessNum );
			break;
		}

		//check max num
		if(iWorkingNum + *m_pSuccessNum >= 50) {
			m_oRsp.set_uint32_result(0);
			m_oRsp.set_str_err_msg("sold out");
			LogWater(LOG_DEBUG,"2 working=%u succeedNum=%u",iWorkingNum,*m_pSuccessNum );
			break;
		}


		//check user
		iRet = CheckUser(strUserName,
				 strUserKey,
				 pRedisConn,
				 mysqlpMysqlConn);
		if(iRet != 0) {
			m_oRsp.set_uint32_result(0);
			m_oRsp.set_str_err_msg("wrong user");
			break;
		}

		//start seckill
		LogWater(LOG_DEBUG,"start SeckillPorc"); 
		SeckillProc(strUserName,strUserKey,pRedisConn,mysqlpMysqlConn);
		LogWater(LOG_DEBUG,"end SeckillPorc"); 

	}while(0);
	
        m_pRedisPool->CloseConnection(pRedisConn);
        m_pMysqlPool->CloseConnection(mysqlpMysqlConn);
	m_oRspWriter.Finish(m_oRsp, Status::OK, this);

    } else {

	LogWater(LOG_DEBUG,"finish state\n"); 
        GPR_ASSERT(m_eStatus == FINISH);
        redisContext *pRedisConn = m_pRedisPool->GetConnection();
        redisCommand(pRedisConn, "lpop %s","work_list");
        m_pRedisPool->CloseConnection(pRedisConn);
        delete this;
    }
}

int SeckillWork::SeckillProcOnMysqlTransaction(const string & strUserName, const string &strUserKey, MYSQL * pMysqlConn)
{
    int iRet = 0;
    int total_count = 10;

    //select for update
    char sSql[200] = {0};
    sprintf(sSql,"SELECT goods_total_count FROM goods_info WHERE goods_id ='1' FOR UPDATE");
    LogWater(LOG_DEBUG,"sql=%s\n",sSql);
    iRet = mysql_query(pMysqlConn, sSql); 
    if(iRet != 0){
        m_oRsp.set_uint32_result(0);
	m_oRsp.set_str_err_msg("system error");
        return -__LINE__;
    } 
    MYSQL_RES *result = mysql_use_result(pMysqlConn); 
    MYSQL_ROW row;
    while((row = mysql_fetch_row(result)) != NULL) {
        if (mysql_num_fields(result) > 0) {
            total_count = atoi(row[0]);
        }
    }
    if (result != NULL) {
        mysql_free_result(result);
    }
    LogWater(LOG_DEBUG,"total_count=%d\n",total_count);

    if(total_count <= 0) {
        m_oRsp.set_uint32_result(0);
	m_oRsp.set_str_err_msg("sold out");
	return 100;  
    }

    // check dup
    sprintf(sSql,"SELECT * FROM order_info WHERE user_name ='%s'", strUserName.c_str());
    iRet = mysql_query(pMysqlConn, sSql);
    if(iRet != 0) {
	LogWater(LOG_ERROR,"mysql=%s",mysql_error(pMysqlConn));
	m_oRsp.set_uint32_result(0);
	m_oRsp.set_str_err_msg("system error");
	    return -__LINE__;
    } 
    result = mysql_use_result(pMysqlConn); 
    while((row = mysql_fetch_row(result)) != NULL) {
        if (mysql_num_fields(result) > 0){
            m_oRsp.set_uint32_result(0);
	    m_oRsp.set_str_err_msg("dup user");
            return -__LINE__;
        }
    }
    if (result != NULL) {
        mysql_free_result(result);
    }

    //update count
    total_count = total_count - 1;
    char updateSql[200] = {'\0'};
    sprintf(updateSql, "UPDATE goods_info SET goods_total_count = %d WHERE goods_id = '1' ", total_count);
    iRet = mysql_query(pMysqlConn,updateSql); 
    if(iRet != 0) {
	    m_oRsp.set_uint32_result(0);
	    m_oRsp.set_str_err_msg("system error");
	    pthread_rwlock_wrlock(m_pRwLock);
	    ++ *m_pFailedNum;
	    pthread_rwlock_unlock(m_pRwLock);
	    return -__LINE__;
    }
    result = mysql_use_result(pMysqlConn); 
    if(result !=  NULL) {
	    mysql_free_result(result);
    }

   // result= mysql_store_result(pMysqlConn);
   // if(result !=  NULL) {
   //         mysql_free_result(result);
   // }


    //update order_info
    LogWater(LOG_DEBUG,"line=%d",__LINE__);
    char insertOSql[200] = {'\0'};
    sprintf(insertOSql, "INSERT INTO order_info(user_name,user_password,goods_id) VALUES ('%s','%s','1')", strUserName.c_str(),strUserKey.c_str());
    iRet = mysql_query(pMysqlConn,insertOSql);
    if(iRet != 0) {
	    m_oRsp.set_uint32_result(0);
	    m_oRsp.set_str_err_msg("system error");
	    pthread_rwlock_wrlock(m_pRwLock);
	    ++ *m_pFailedNum;
	    pthread_rwlock_unlock(m_pRwLock);
	    return -__LINE__;
    }
    result = mysql_use_result(pMysqlConn); 
    if(result !=  NULL) {
	    mysql_free_result(result);
    }

  //  result= mysql_store_result(pMysqlConn);
  //  if(result !=  NULL) {
  //          LogWater(LOG_DEBUG,"line=%d\n",__LINE__);
  //          mysql_free_result(result);
  //  }


    m_oRsp.set_uint32_result(1);
    ++ *m_pSuccessNum;
    LogWater(LOG_DEBUG,"mysql succeed\n");
    return 0;
}

int SeckillWork::SeckillProcOnMysql(const string & strUserName,
        const string &strUserKey,
        MYSQL * pMysqlConn)
{
    if(NULL == pMysqlConn) {
        return -__LINE__;
    }

    const int ON = 1;
    const  int OFF = 0;
    int iRet = 0;

    mysql_autocommit(pMysqlConn,OFF);
    iRet = SeckillProcOnMysqlTransaction(strUserName,strUserKey,pMysqlConn);
    if(0 == iRet ) {
        //commit
        mysql_commit(pMysqlConn); 
    } else if(100 == iRet) {
        //rollback
        mysql_rollback(pMysqlConn);
    }else if(iRet < 0) {
        //rollback
        mysql_rollback(pMysqlConn);
        //update failed num
        pthread_rwlock_wrlock(m_pRwLock);
        ++ *m_pFailedNum;
        pthread_rwlock_unlock(m_pRwLock);

    }
    mysql_autocommit(pMysqlConn,ON);
}

int SeckillWork::SeckillProc(const string & strUserName,
        const string &strUserKey,
        redisContext * pRedisConn,
        MYSQL *pMysqlConn)
{
    int iRet = 0;
    if( NULL !=  pRedisConn) {
        int iTryCount = 0;
        iRet = SeckillProcOnRedis(strUserName,strUserKey, pRedisConn,pMysqlConn,iTryCount);
	if(iRet != 0) {
		LogWater(LOG_ERROR,"SeckillProc failed\n");
	}else {
		LogWater(LOG_DEBUG,"SeckillProc succeed\n");
	}
    }else {
        if(NULL != pMysqlConn) {
            SeckillProcOnMysql(strUserName,strUserKey,pMysqlConn);
        }
    }
    return 0;
}

int  SeckillWork::SeckillProcOnRedis(const string & strUserName,
        const string &strUserKey,
        redisContext * pRedisConn,
        MYSQL *pMysqlConn,
        int iRetryCount)
{ 

    ++ iRetryCount;
    redisReply *pWatchReply = (redisReply *)redisCommand(pRedisConn,"WATCH %s","total_count");
    if(pWatchReply  == NULL) {
        return -__LINE__;
    }else if(!(pWatchReply->type == REDIS_REPLY_STATUS && strcasecmp(pWatchReply->str,"OK") == 0)) {
        freeReplyObject(pWatchReply);
        return -__LINE__;
    }
    int iRet = SeckillProcOnRedisTransaction(strUserName,strUserKey, pRedisConn, iRetryCount);
    redisCommand(pRedisConn, "UNWATCH");

    if(0 == iRet) {
        //write mysql
	    iRet = SeckillProcOnMysql(strUserName,strUserKey,pMysqlConn);
    }else if(iRet < 0) {
	    //retry if failed
	    if( iRetryCount < g_iMaxTry ) {
		    SeckillProcOnRedis(strUserName,
				    strUserKey,
				    pRedisConn,
				    pMysqlConn, 
				    iRetryCount);
	    }else {

        }
    }else if(100 == iRet) {
        //redis sold out. get left from mysql
        pthread_rwlock_wrlock(m_pRwLock);
        if (*m_pFailedNum > 0) {
            std::string failedC = std::to_string(*m_pFailedNum);
            redisReply *setCreply = (redisReply *)redisCommand(pRedisConn, "SET total_count %s", failedC.c_str());
            if (setCreply != NULL 
                    &&  setCreply->type == REDIS_REPLY_STATUS
                    && (strcasecmp(setCreply->str,"OK") == 0)) {
                *m_pFailedNum = 0;
            }
        }
        pthread_rwlock_unlock(m_pRwLock);

    }else if(200 == iRet) {
        //dup user
        return 0;

    }else {
        return -__LINE__;
    }
    return 0;
}

int SeckillWork::SeckillProcOnRedisTransaction(const string & strUserName,
        const string &strUserKey,
        redisContext * pRedisConn,
        int iRetryCount)
{
    std::cout << "satrt shopping on redis!" << std::endl;
    int iCurCount = 0;
    redisReply *pCountReply = (redisReply *)redisCommand(pRedisConn, "GET %s", "total_count");
    if(NULL == pCountReply) {
        return -__LINE__;
    }
    iCurCount = atoi(pCountReply->str);
    freeReplyObject(pCountReply);
    LogWater(LOG_DEBUG,"iCurCount=%d\n",iCurCount);


    //redis sold out
    if(iCurCount <= 0) {
        m_oRsp.set_uint32_result(0);
        std::cout << "seckill failed! The goods has sold out!" << std::endl;
        return 100; //hold
    }

    //check dup
    redisReply *pReply = (redisReply *)redisCommand(pRedisConn, "HGET order_info %s",strUserName.c_str());
    if(NULL == pReply) {
	std::cout<<"NULL == pReply"<<endl;
        return -__LINE__;
    }
    if(pReply->type == REDIS_REPLY_STRING && pReply->str != NULL) {
        freeReplyObject(pReply);
	std::cout<<"dup seckill"<<endl;
        return 200; //hold
    }
    freeReplyObject(pReply);

    //start 
    redisReply *multiReply = NULL;
    multiReply  = (redisReply *)redisCommand(pRedisConn, "MULTI");
    if(NULL ==  multiReply) {
        freeReplyObject(multiReply);
        return -__LINE__;
    }

    if(!(multiReply->type == REDIS_REPLY_STATUS  && (strcasecmp(multiReply->str,"OK") == 0)) ){
        freeReplyObject(multiReply);
        return -__LINE__;
    }

    //update count
    redisCommand(pRedisConn, "DECR %s", "total_count");
    redisCommand(pRedisConn, "HMSET order_info  %s %s",strUserName.c_str(), strUserKey.c_str());

    redisReply *execReply = NULL;
    execReply  = (redisReply *)redisCommand(pRedisConn, "EXEC");
    if(NULL == execReply) {
        freeReplyObject(execReply);
        return -__LINE__;
    }

    if(!(execReply->type == REDIS_REPLY_ARRAY && (execReply->elements > 1))) {
        freeReplyObject(execReply);
        return -__LINE__;
    }

    return 0;
}

int SeckillWork::CheckUserOnRedis(const string & strUserName,
        const string &strUserKey,
        redisContext * pRedisConn)
{
    if(NULL ==  pRedisConn) {
        return -10;
    }

    redisReply *pReply = (redisReply *)redisCommand(pRedisConn, "HGET usr_info %s", strUserName.c_str());
    if (pReply != NULL && REDIS_REPLY_STRING == pReply->type) {
        std::string strKey(pReply->str);
        if (strUserKey == strKey) {
            return 1;
        }
    }
    freeReplyObject(pReply );
    return 0;
}

int SeckillWork::CheckUserOnMysql(const string & strUserName,
        const string &strUserKey,
        MYSQL * pMysqlConn)
{
    if(NULL == pMysqlConn) {
        return -__LINE__;
    }

    int iRet = 0;
    char sSql[200] = {'\0'};
    sprintf(sSql,"SELECT user_password FROM user_info WHERE user_name ='%s'",strUserName.c_str());
    std::cout<< sSql <<std::endl;
    iRet = mysql_query(pMysqlConn, sSql);
    if(iRet != 0) {
        return -__LINE__;
    } 

    MYSQL_RES *pResult = mysql_use_result(pMysqlConn); 
    if(NULL == pResult) {
        return 0;
    }

    MYSQL_ROW row;
    while((row = mysql_fetch_row(pResult)) != NULL) {
        if (mysql_num_fields(pResult) > 0) {
            std::string strKey(row[0]);
            if (strKey == strUserKey) {
                return 1;
            }
        }
    }
    mysql_free_result(pResult);
    return 0;
}

int SeckillWork::CheckUser(const string & strUserName,
                const string &strUserKey,
                redisContext * pRedisConn,
                MYSQL * pMysqlConn)
{
    int iRet = -10;
    if (pRedisConn !=  NULL) {
        iRet = CheckUserOnRedis(strUserName,strUserKey, pRedisConn);
    }

    if(iRet < 0) {
        iRet = CheckUserOnMysql(strUserName,strUserKey, pMysqlConn);
    }
    return iRet;
}

