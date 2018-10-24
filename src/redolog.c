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
#include <stdarg.h>
#include <unistd.h>

/** 创建文件 */
static int createRedoLogFile(const char * filename){
	return open(filename, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

/** 打开文件 */
static int openRedoLogFile(const char * filename){
	return open(filename, O_RDWR);
}

static void freeOperateTuple(OperateTuple *operateTuple){
	freeList(operateTuple->objects);
	free(operateTuple);
}

void operateListForEach(void* value, void* args){
	OperateTuple *operateTuple = (OperateTuple *)value;
	RedoLog *redoLog = (RedoLog *)args;
	redoLog->persistenceFunction(redoLog->fd, operateTuple);
	freeOperateTuple(operateTuple);
}

/** 进行持久化 */
void doPersistence(RedoLog *redoLog, List *operateList){
	foreachList(operateList, operateListForEach, (void*)redoLog);
	freeList(operateList);
}

/** 检查是否需要进行持久化 */
int checkNeedPersistence(RedoLog* redoLog){
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
		}
		pthread_mutex_unlock(&redoLog->statusMutex);
		pthread_cleanup_pop(0);

		if(redoLog->status == persistence){
			doPersistence(redoLog, operateList);
			if (redoLog->status == persistence){
				redoLog->status = normal;
			}
			if(redoLog->flushStrategy==synchronize){
				//同步策略立即通知工作线程
				pthread_cond_signal(&redoLog->statusCond);
			}
		}
	}
}

/** 初始化一个RedoLog，不包括打开文件 */
static RedoLog *initRedoLog(
	char *filename,
	void (*persistenceFunction)(int, struct OperateTuple *),
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg)
{
	//拷贝文件名
	RedoLog *redoLog = (RedoLog *)malloc(sizeof(RedoLog));
	newAndCopyByteArray((uint8**)&redoLog->filename,(uint8*)filename, strlen(filename) + 1);
	//成员变量
	redoLog->flushStrategy = flushStrategy;
	redoLog->flushStrategyArg = flushStrategyArg;
	redoLog->lastFlushTime = currentTimeMillis();
	redoLog->operateList = makeList();
	redoLog->status = normal;
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

OperateTuple *makeOperateTuple(uint8 type, ...){
	va_list valist;
	int len = 0;
	switch (type)
	{
		case 1:
			/* 表示插入：操作数长度为2 */
			len = 2;
			break;
		case 2:
			/* 表示移除：操作数长度为1 */
			len = 1;
			break;
		case 3:
			/* 表示精确移除：操作数长度为2 */
			len = 2;
			break;
		default:
			return NULL;
	}
	OperateTuple* operateTuple = calloc(1, sizeof (OperateTuple));
	operateTuple->type = type;
	operateTuple->objects = makeList();
	va_start(valist, type);
	for(int i=0; i<len; i++){
		addList(operateTuple->objects, va_arg(valist, void*));
	}
	va_end(valist);
	return operateTuple;
}

RedoLog *makeRedoLog(
	char *filename,
	void (*persistenceFunction)(int, struct OperateTuple *),
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg)
{
	int fd = createRedoLogFile(filename);
	if(fd<0) { return NULL; }
	RedoLog *redoLog = initRedoLog(filename, persistenceFunction,flushStrategy, flushStrategyArg);
	redoLog->fd = fd;
	return redoLog;
}

RedoLog *loadRedoLog(
	char *filename,
	void (*persistenceFunction)(int, struct OperateTuple *),
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg)
{
	int fd = openRedoLogFile(filename);
	if(fd<0) { return NULL; }
	RedoLog *redoLog = initRedoLog(filename, persistenceFunction,flushStrategy, flushStrategyArg);
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
	}
	pthread_mutex_unlock(&redoLog->statusMutex);
	pthread_cleanup_pop(0);
	if (redoLog->status != finish){
		pthread_cond_signal(&redoLog->statusCond);
	}
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