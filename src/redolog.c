/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: redolog.c
 * @description: 重做日志相关实现
 * @author: Rectcircle
 * @version: 1.0
 * @date 2018-10-21 
 ******************************************************************************/

#include "redolog.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/** 创建文件：以O_APPEND方式 */
static int createRedoLogFile(const char * filename){
	return open(filename, O_APPEND | O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

/** 打开文件：以O_APPEND方式 */
static int openRedoLogFile(const char * filename){
	return open(filename, O_APPEND | O_RDWR);
}

void operateListForEach(void* value, void* args){
	OperateTuple *operateTuple = (OperateTuple *)value;
	RedoLog *redoLog = (RedoLog *)args;
	redoLog->persistenceFunction(redoLog, operateTuple);
	redoLog->freeOperateTuple(operateTuple);
}

/** 进行持久化 */
void doPersistence(RedoLog *redoLog, List *operateList){
	foreachList(operateList, operateListForEach, (void*)redoLog);
	clearList(operateList);
}

/** 检查是否需要进行持久化 */
int checkNeedPersistence(RedoLog* redoLog){
	if(redoLog->operateList->length==0){
		return 0;
	}
	if (redoLog->flushStrategy == synchronize){
		//同步策略：任何情况返回true
		return 1;
	} else if(redoLog->flushStrategy == definiteTime){
		//定时策略：当相差时间大于阈值
		uint64 now = currentTimeMillis();
		return now-redoLog->lastFlushTime >= redoLog->flushStrategyArg;
	} else if(redoLog->flushStrategy == sizeThreshold){
		//阈值策略：已经存在超过阈值数量个日志
		return redoLog->operateList->length>=redoLog->flushStrategyArg;
	}
	return 1;
}

/** 持久化任务 */
static void persistenceTask(RedoLog* redoLog){
	List *operateList = makeList();
	while(1){
		pthread_cleanup_push((void *)pthread_mutex_unlock, &redoLog->statusMutex);
		pthread_mutex_lock(&redoLog->statusMutex);
		pthread_testcancel();
		//如果设置为finish状态，处理完剩余的直接结束
		if (redoLog->status == finish){
			addListToList(operateList, redoLog->operateList);
			doPersistence(redoLog, operateList);
			pthread_exit(NULL);
		}
		if(redoLog->status == normal && checkNeedPersistence(redoLog)){
			//如果需要进行持久化
			//切换状态
			redoLog->status = persistence;
			//清空并拷贝列表
			addListToList(operateList, redoLog->operateList);
		} else {
			//不需要进行持久化，wait
			pthread_cond_wait(&redoLog->statusCond, &redoLog->statusMutex);
			pthread_testcancel();
		}
		pthread_mutex_unlock(&redoLog->statusMutex);
		pthread_cleanup_pop(0);

		if(redoLog->status == persistence){
			doPersistence(redoLog, operateList);
			redoLog->lastFlushTime = currentTimeMillis();
			if (redoLog->status == persistence){
				redoLog->status = normal;
			}
			// if(redoLog->flushStrategy==synchronize){
			//同步策略立即通知工作线程
			pthread_cond_signal(&redoLog->statusCond);
			// }
		}
	}
}

/** 初始化一个RedoLog，不包括打开文件 */
static RedoLog *initRedoLog(
	char *filename,
	void *env,
	uint64 operateListMaxSize,
	RedoPersistenceFunction persistenceFunction,
	FreeOperateTupleFunction freeOperateTuple,
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg)
{
	if (flushStrategy == sizeThreshold && operateListMaxSize < flushStrategyArg)
	{
		//如果允许将造成死锁
		return NULL;
	}
	//拷贝文件名
	RedoLog *redoLog = (RedoLog *)malloc(sizeof(RedoLog));
	newAndCopyByteArray((uint8**)&redoLog->filename,(uint8*)filename, strlen(filename) + 1);
	//成员变量
	redoLog->operateListMaxSize = operateListMaxSize;
	redoLog->flushStrategy = flushStrategy;
	redoLog->flushStrategyArg = flushStrategyArg;
	redoLog->lastFlushTime = currentTimeMillis();
	redoLog->operateList = makeList();
	redoLog->status = normal;
	redoLog->persistenceFunction = persistenceFunction;
	redoLog->freeOperateTuple = freeOperateTuple;
	redoLog->env = env;
	//初始化线程相关内容
	pthread_cond_init(&redoLog->statusCond, NULL);
	pthread_mutexattr_init(&redoLog->statusAttr);
	pthread_mutexattr_settype(&redoLog->statusAttr, PTHREAD_MUTEX_RECURSIVE_NP);
	pthread_mutex_init(&redoLog->statusMutex, &redoLog->statusAttr);
	//启动线程后台持久化线程
	pthread_create(&redoLog->persistenceThread, NULL, (void *)persistenceTask, (void *)redoLog);
	return redoLog;
}

/*****************************************************************************
 * 公开API
 ******************************************************************************/

RedoLog *makeRedoLog(
	char *filename,
	void *env,
	uint64 operateListMaxSize,
	RedoPersistenceFunction persistenceFunction,
	FreeOperateTupleFunction freeOperateTuple,
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg)
{
	int fd = createRedoLogFile(filename);
	if(fd<0) { return NULL; }
	RedoLog *redoLog = initRedoLog(filename, env, operateListMaxSize, persistenceFunction, freeOperateTuple, flushStrategy, flushStrategyArg);
	if (redoLog == NULL)
	{
		close(fd);
		return NULL;
	}
	redoLog->fd = fd;
	return redoLog;
}

RedoLog *loadRedoLog(
	char *filename,
	void *env,
	uint64 operateListMaxSize,
	RedoPersistenceFunction persistenceFunction,
	FreeOperateTupleFunction freeOperateTuple,
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg)
{
	int fd = openRedoLogFile(filename);
	if(fd<0) { return NULL; }
	RedoLog *redoLog = initRedoLog(filename, env, operateListMaxSize, persistenceFunction, freeOperateTuple, flushStrategy, flushStrategyArg);
	if(redoLog==NULL){
		close(fd);
		return NULL;
	}
	redoLog->fd = fd;
	return redoLog;
}

void appendRedoLog(RedoLog *redoLog, OperateTuple *ops){
	pthread_cleanup_push((void *)pthread_mutex_unlock, &redoLog->statusMutex);
	pthread_mutex_lock(&redoLog->statusMutex);
	//如果设置为策略为同步且正在进行持久化要等待
	if (redoLog->status == persistence && redoLog->flushStrategy==synchronize){
		pthread_cond_wait(&redoLog->statusCond, &redoLog->statusMutex);
	}
	if(redoLog->status != finish){
		addList(redoLog->operateList, (void *)ops);
		int flag = redoLog->operateList->length >= redoLog->operateListMaxSize;
		pthread_cond_signal(&redoLog->statusCond);
		if (flag || redoLog->flushStrategy == synchronize){
			pthread_cond_wait(&redoLog->statusCond, &redoLog->statusMutex);
		}
	}
	pthread_mutex_unlock(&redoLog->statusMutex);
	pthread_cleanup_pop(0);
}

void freeRedoLog(RedoLog *redoLog){
	pthread_cleanup_push((void *)pthread_mutex_unlock, &redoLog->statusMutex);
	pthread_mutex_lock(&redoLog->statusMutex);
	redoLog->status = finish;
	pthread_cond_signal(&redoLog->statusCond);
	pthread_mutex_unlock(&redoLog->statusMutex);
	pthread_cleanup_pop(0);
	pthread_join(redoLog->persistenceThread, NULL);
	close(redoLog->fd);
	free(redoLog->filename);
	freeList(redoLog->operateList);
	free(redoLog);
}

void forceFreeRedoLog(RedoLog *redoLog){
	pthread_cancel(redoLog->persistenceThread);
	close(redoLog->fd);
	free(redoLog->filename);
	freeList(redoLog->operateList);
	free(redoLog);
}

void forceFreeRedoLogAndUnlink(RedoLog *redoLog){
	pthread_cancel(redoLog->persistenceThread);
	close(redoLog->fd);
	unlink(redoLog->filename);
	free(redoLog->filename);
	freeList(redoLog->operateList);
	free(redoLog);
}