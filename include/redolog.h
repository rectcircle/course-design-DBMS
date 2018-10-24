/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * 重做日志相关内容：
 * 
 * 采用单独的线程异步写入磁盘，写入策略分为三种
 * 1. 立即写入
 * 2. 定时写入
 * 3. 阈值写入
 * 
 * OperateTuple内存管理方式：外部创建，内部释放
 * RedoLog内存管理方式：内部创建，内部释放，内部自制
 * 
 * @filename: redolog.h
 * @description: 重做日志相关定义
 * @author: Rectcircle
 * @version: 1.0
 * @date 2018-10-21 
 ******************************************************************************/
#pragma once
#ifndef __REDOLOG_H__
#define __REDOLOG_H__

#include "global.h"
#include "util.h"
#include <malloc.h>
#include <pthread.h>

/*****************************************************************************
 * 宏定义
 ******************************************************************************/

/** 插入操作 ，提供key,value*/
#define OPERATETUPLE_TYPE_INSERT 1
/** 删除操作，仅提供key */
#define OPERATETUPLE_TYPE_REMOVE1 2
/** 精确删除操作，提供key,value */
#define OPERATETUPLE_TYPE_REMOVE2 3

/*****************************************************************************
 * 枚举定义
 ******************************************************************************/

enum RedoLogStatus
{
	/** 持久化线程正在wait */
	normal,
	/** 持久化线程正在进行持久化 */
	persistence,
	/** 释放内存阶段 */
	finish
};

enum RedoFlushStrategy
{
	/** 同步策略，插入一个操作元组，持久化一个元组，然后返回 */
	synchronize,
	/** 定时策略 */
	definiteTime,
	/** 阈值策略 */
	sizeThreshold
};

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

/**
 * 重做日志单元操作元组
 */
typedef struct OperateTuple
{
	/** 操作类型 */
	uint8 type;
	/** 操作对象，具体内容由type定义，类型List<void*> */
	struct List* objects;
} OperateTuple;

/**
 * 重做日志
 */
typedef struct RedoLog
{
	/** 重做日志文件名 */
	char* filename;
	/** 重做日志对应文件描述符 */
	int fd;
	/** 重做日志写入磁盘策略 */
	enum RedoFlushStrategy flushStrategy;
	/** 写入策略辅助参数 */
	/** 当flushStrategy==definiteTime有效，表示倒计时的时间 */
	/** 当flushStrategy==definiteTime有效，表示倒计时的时间 */
	uint64 flushStrategyArg;
	/** 上次持久化（或创建文件）的时间：毫秒级时间戳 */
	uint64 lastFlushTime;
	/** 操作元组列表：操作中的元组List<OperateTuple*> */
	struct List *operateList;
	/** 重做元组状态 */
	volatile enum RedoLogStatus status;
	/** 持久化函数 */
	void (*persistenceFunction)(int, struct OperateTuple*);
	/** 条件变量，用于控制并发 */
	pthread_cond_t statusCond;
	/** 用于互斥更改状态 */
	pthread_mutex_t statusMutex;
	/** 设置成可重入 */
	pthread_mutexattr_t statusAttr;
	/** 持久化线程 */
	pthread_t persistenceThread;
} RedoLog;

/*****************************************************************************
 * 公开API
 ******************************************************************************/

/** 
 * 创建一个OperateTuple
 * @param type 操作类型
 * @param {...} 可变参数操作对象
 */
OperateTuple *makeOperateTuple(uint8 type, ...);

/**
 * 释放OperateTuple，私有函数
 */
// void freeOperateTuple(OperateTuple * operateTuple);

/**
 * 创建一个重做日志
 * @param filename 重做日志文件名
 * @param persistenceFunction 一个函数，针对每一个操作日志记录做持久化
 * @param flushStrategy 刷磁盘策略
 * @param flushStrategyArg 刷磁盘策略的参数
 * @return 一个可用的重做日志
 */
RedoLog *makeRedoLog(
	char *filename,
	void (*persistenceFunction)(int, struct OperateTuple *),
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg);

/**
 * 从磁盘中加载一个重做日志
 * @param filename 重做日志文件名
 * @param persistenceFunction 一个函数，针对每一个操作日志记录做持久化
 * @param flushStrategy 刷磁盘策略
 * @param flushStrategyArg 刷磁盘策略的参数
 * @return 一个可用的重做日志
 */
RedoLog *loadRedoLog(
	char *filename,
	void (*persistenceFunction)(int, struct OperateTuple *),
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg);

/**
 * 想重做日志中添加一个操作
 * @param redoLog 一个可用的重做日志
 * @param ops 一个操作
 */
void appendRedoLog(RedoLog *redoLog, OperateTuple* ops);

/**
 * 等待重做日志刷磁盘线程完毕，释放内存
 * @param redoLog 一个可用的重做日志
 */
void freeRedoLog(RedoLog *redoLog);

/**
 * 从文件中读取重做日志
 * @param redoLog 一个可用的重做日志
 * @return {List<OperateTuple>} 重做操作列表
 */
List *getOperateListFromFile(RedoLog *redoLog);

/**
 * 索引引擎持久化函数
 * @param fd 文件描述符
 * @param op 一个重做操作
 */
void indexEngineRedoLogPersistenceFunction(int fd, struct OperateTuple *op);

#endif