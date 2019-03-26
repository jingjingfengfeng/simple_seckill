/*
* ==============================================
*        Filename: MysqlPoolApi.h
*        Description: 
* ==============================================
*/
#ifndef _MYSQLPOOLAPI_H_
#define _MYSQLPOOLAPI_H_

#include<iostream>
#include<utility>
#include<vector>
#include<queue>
#include<map>
#include<string>
#include<mutex>
#include<thread>

#include "mysql/mysql.h"

typedef std::map<const std::string,std::vector<const char* > > SQLRET; 

class MysqlPool
{
    public:
    ~MysqlPool();
    int InitPoolConnection(const char * sHost,
             const char * sUser,
             const char * sPwd,
             const char * sDataBase,
             unsigned int wPort,
             const char * sSocket,
             unsigned int wClientFlag,
             unsigned int wMaxConnect);

    static MysqlPool* GetInstance(); 
    int  ExecMysqlCmd(const char * sSql,SQLRET & mapRet);
    MYSQL* GetConnection();
    int CloseConnection(MYSQL* pConnection);

    private:
      MysqlPool();  

      MYSQL* CreateConnection(); 

      int DestoryConnection();
      int DestoryConnPool();
      
    private:
      std::queue<MYSQL*> m_pQueueConn; 
      const char*   m_sHost;                      
      const char*   m_sUser;                     
      const char*   m_sPwd;                      
      const char*   m_sDataBase;                 
      unsigned int  m_uPort;                     
      const char*   m_sSocket;                    
      unsigned long m_uClientFlag;               
      unsigned int  m_uMaxSize;                
      unsigned int  m_uCurSize;                
      static std::mutex m_stGeneLock;           
      static std::mutex m_stPoolLock;             
      static MysqlPool* m_pMysqlPool;        

};

#endif
