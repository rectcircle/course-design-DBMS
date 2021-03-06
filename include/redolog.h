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
 * OperateTuple内存管理方式：make创建，内部释放
 * OperateTuple的参数：外部创建，内部释放
 * RedoLog内存管理方式：make创建，内部释放，内部自制
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
 * 类型定义
 ******************************************************************************/
struct RedoLog;		 //去除警告用
struct OperateTuple; //去除警告用
typedef void (*RedoPersistenceFunction)(struct RedoLog *, struct OperateTuple *);
typedef void (*FreeOperateTupleFunction)(struct OperateTuple *);

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
	/** 内存中操作列表的最大尺寸，当超过了这个大小主线程将阻塞 */
	uint64 operateListMaxSize;
	/** 操作元组列表：操作中的元组List<OperateTuple*> */
	struct List *operateList;
	/** 重做元组状态 */
	volatile enum RedoLogStatus status;
	/** 环境，该重做日志工作对象：针对索引引擎还是hash引擎？ */
	void* env;
	/** 持久化函数 */
	// void (*persistenceFunction)(struct RedoLog*, struct OperateTuple *);
	RedoPersistenceFunction persistenceFunction;
	/** OperateTuple清理函数 */
	FreeOperateTupleFunction freeOperateTuple;
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
 * @param operateListMaxSize 内存最大持久尺寸：超过这个尺寸将阻塞主线程
 * @param persistenceFunction 一个函数，针对每一个操作日志记录做持久化
 * @param flushStrategy 刷磁盘策略
 * @param flushStrategyArg 刷磁盘策略的参数
 * @return 一个可用的重做日志
 */
RedoLog *makeRedoLog(
	char *filename,
	void *env,
	uint64 operateListMaxSize,
	RedoPersistenceFunction persistenceFunction,
	FreeOperateTupleFunction freeOperateTuple,
	enum RedoFlushStrategy flushStrategy,
	uint64 flushStrategyArg);

/**
 * 从磁盘中加载一个重做日志
 * @param filename 重做日志文件名
 * @param operateListMaxSize 内存最大持久尺寸：超过这个尺寸将阻塞主线程
 * @param persistenceFunction 一个函数，针对每一个操作日志记录做持久化
 * @param flushStrategy 刷磁盘策略
 * @param flushStrategyArg 刷磁盘策略的参数
 * @return 一个可用的重做日志
 */
RedoLog *loadRedoLog(
	char *filename,
	void *env,
	uint64 operateListMaxSize,
	RedoPersistenceFunction persistenceFunction,
	FreeOperateTupleFunction freeOperateTuple,
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
 * 强制释放RedoLog（不等待刷磁盘线程完成）
 * @param redoLog 一个可用的重做日志
 */
void forceFreeRedoLog(RedoLog* redoLog);

/**
 * 强制释放RedoLog（不等待刷磁盘线程完成），并删除文件
 * @param redoLog 一个可用的重做日志
 */
void forceFreeRedoLogAndUnlink(RedoLog *redoLog);

#endif