/*
* ==============================================
*        Filename: MysqlPoolApi.cpp
*        Description: 
* ==============================================
*/

#include "MysqlPoolConnection.h"

MysqlPool* MysqlPool::m_pMysqlPool = NULL;
std::mutex MysqlPool::m_stGeneLock;
std::mutex MysqlPool::m_stPoolLock;

 MysqlPool:: MysqlPool(){}
MysqlPool* MysqlPool::GetInstance()
{
    if (NULL == m_pMysqlPool) { 
         m_stGeneLock.lock();
        if (NULL == m_pMysqlPool) {
            m_pMysqlPool = new MysqlPool();
        }
        m_stGeneLock.unlock();
    }
    return m_pMysqlPool;
}

int MysqlPool::InitPoolConnection(const char * sHost,
             const char * sUser,
             const char * sPwd,
             const char * sDataBase,
             unsigned int wPort,
             const char * sSocket,
             unsigned int uClientFlag,
             unsigned int uMaxConnect)
{
    m_sHost = sHost;
    m_sUser = sUser;                     
    m_sPwd  = sPwd;                      
    m_sDataBase = sDataBase;                 
    m_uPort = wPort;                     
    m_sSocket = sSocket;                    
    m_uClientFlag = uClientFlag;               
    m_uMaxSize = uMaxConnect;   
    m_uCurSize = 0;

    return 0;
}

int MysqlPool::CloseConnection(MYSQL* pConnection)
{
    if(pConnection != NULL) {
        m_stPoolLock.lock();
        m_pQueueConn.push(pConnection);
        m_stPoolLock.unlock();
    }
    return 0;
}

int MysqlPool::DestoryConnPool()
{
    while (m_pQueueConn.size() != 0) {
        mysql_close(m_pQueueConn.front());
        m_pQueueConn.pop();
        m_uCurSize--;
    }
    return 0;
}


MysqlPool::~MysqlPool() 
{
    DestoryConnPool();
}

MYSQL* MysqlPool::GetConnection()
{
    m_stPoolLock.lock();
    int iRet = 0;
    MYSQL *pConnection = NULL;

    //del invalid conn
    while(false == m_pQueueConn.empty()) {
        pConnection = m_pQueueConn.front();
        iRet = mysql_ping(pConnection);

        if(iRet != 0) {
            mysql_close(pConnection);
            m_pQueueConn.pop();
            m_uCurSize --;
        }else {
            break;
        }
    }

    if(false == m_pQueueConn.empty()) {
        pConnection = m_pQueueConn.front();
        m_pQueueConn.pop();
    }

    //create new connection
    if(NULL == pConnection) {
        if(m_uCurSize < m_uMaxSize) {
            pConnection =  CreateConnection(); 
        }else {
            std::cerr << "mysql connections is over size! cur_size:" << m_uCurSize<<" max:" << m_uMaxSize<< std::endl;
        }
    }

    m_stPoolLock.unlock();
    return pConnection;
}

MYSQL* MysqlPool::CreateConnection()
{
  MYSQL* pConnection = NULL;
  pConnection = mysql_init(pConnection);
  if(NULL ==  pConnection) {
      return NULL; 
  }

  pConnection = mysql_real_connect(pConnection,
                                   m_sHost,
                                   m_sUser,
                                   m_sPwd,
                                   m_sDataBase, 
                                   m_uPort,
                                   m_sSocket,
                                   m_uClientFlag);
  if(NULL == pConnection) {
      return NULL; 
  }

  m_uCurSize++;

  return pConnection; 

}
