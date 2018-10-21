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

#define REDOLOG_STATUS_NORMAL 0

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
	/** 上次持久化（或创建文件）的时间 */
	uint64 lastFlushTime;
	/** 操作元组列表：操作中的元组 */
	struct List *operateListWork;
	/** 重做元组状态 */
	volatile enum RedoLogStatus status;
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
 * 创建一个重做日志
 * @param filename 重做日志文件名
 * @param flushStrategy 刷磁盘策略
 * @return 一个可用的重做日志
 */
RedoLog *makeRedoLog(char *filename, enum RedoFlushStrategy flushStrategy);

/**
 * 从磁盘中加载一个重做日志
 * @param filename 重做日志文件名
 * @param flushStrategy 刷磁盘策略
 * @return 一个可用的重做日志
 */
RedoLog *loadRedoLog(char *filename, enum RedoFlushStrategy flushStrategy);

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

#endif