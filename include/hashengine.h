/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * 实现一个基于Hash的存储引擎，支持如下内容：
 * 创建或加载一个Hash引擎
 * 对数据进行增删改查
 * 启动故障检测数据恢复
 * 支持读写分离
 * 
 * 一些限制：
 * 
 * 内存管理方式，主要针对IndexTreeNode：
 * 
 * @filename: hashengine.h
 * @description: Hash存储引擎API
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-12-08
 ******************************************************************************/
#pragma once
#ifndef __HASHENGINE_H__
#define __HASHENGINE_H__

#include <pthread.h>

#include "global.h"
#include "util.h"
#include "lrucache.h"
#include "hashmap.h"

/*****************************************************************************
 * 宏定义
 ******************************************************************************/

/*****************************************************************************
 * 枚举定义
 ******************************************************************************/

enum HashEnginePersistenceStatus
{
	/** 不在进行 */
	None,
	// /** 正在进行writecache切换 */
	// Before,
	/** 持久化线程正在进行持久化 */
	Doing,
	/** 持久化之后的清理工作 */
	After
};

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

/**
 * 记录的位置
 */
typedef struct RecordLocation
{
	/** 文件偏移量 */
	uint64 position;
	/** 在内存cache中的id */
	uint64 id;
} RecordLocation;

/**
 * 记录内容
 */
typedef struct Record
{
	/** 记录版本号：从1开始，0保留 */
	uint64 version;
	/** keyLen */
	uint32 keyLen;
	/** valueLen */
	uint32 valueLen;
	/** key */
	uint8 *key;
	/** value */
	uint8 *value;
} Record;

/**
 * 索引文件
 */
typedef struct HashEngine
{
	/** 索引文件位置 */
	char *filename;
	/** 数据文件描述符，用于写，没有进行碎片整理时指向filename, 进行碎片整理时指向newFilename */
	int wfd;
	/** 数据文件描述符，用于读，指向filename */
	int rfd;
	/** 数据文件描述符，用于读，指向newFilename */
	int newrfd;
	/** id种子 */
	uint64 idSeed;
	/** filename的索引 <key, RecordLocation> */
	struct HashMap *hashMap;
	/** 读缓存 <RecordLocation.id, Record> */
	struct LRUCache *readCache;
	/** 写缓存（工作中） <RecordLocation.id, Record>*/
	struct LRUCache *writeCache;
	/** 写缓存（冻结中的） <RecordLocation.id, Record>*/
	struct LRUCache *freezeWriteCache;
	/** 持久化状态 */
	enum HashEnginePersistenceStatus persistenceStatus;
	/** 重做日志相关配置：内存最大持久尺寸 */
	uint64 operateListMaxSize;
	/** 重做日志相关配置：重做日志刷新策略 */
	enum RedoFlushStrategy flushStrategy;
	/** 重做日志相关配置：重做日志刷新策略参数 */
	uint64 flushStrategyArg;
	/** 工作中的RedoLog */
	struct RedoLog *redoLogWork;
	/** 冻结的RedoLog */
	struct RedoLog *redoLogFreeze;
	/** 条件变量，用于控制并发 */
	pthread_cond_t statusCond;
	/** 用于互斥更改状态 */
	pthread_mutex_t statusMutex;
	/** 设置成可重入 */
	pthread_mutexattr_t statusAttr;
	/** 持久化线程 */
	pthread_t persistenceThread;
} HashEngine;

/*****************************************************************************
 * 公开API
 ******************************************************************************/

/**
 * 创建一个Hash引擎文件
 * @param filename 文件名
 * @param hashMapCap 内存索引尺寸
 * @param cacheCap 缓存容量
 * @return {HashEngine *} 一个HashEngine结构
 */
HashEngine *makeHashEngine(const char *filename, uint32 hashMapCap, uint64 cacheCap);

/**
 * 加载一个Hash引擎文件
 * @param filename 文件名
 * @param hashMapCap 内存索引尺寸
 * @param cacheCap 缓存容量
 * @return {HashEngine *} 一个HashEngine结构
 */
HashEngine *loadHashEngine(const char *filename, uint32 hashMapCap, uint64 cacheCap);

/**
 * 释放HashEngine
 */
void freeHashEngine(HashEngine* engine);

/*****************************************************************************
 * 增删查（改通过查删增实现）
 ******************************************************************************/

/**
 * 从Hash引擎中查找key对应的value，只可能有一个
 * @param engine HashEngine
 * @param key 要查找的key
 * @return {Array} value 带长度的字节数组数组类型
 */
Array getHashEngine(HashEngine *engine, uint32 keyLen, uint8 *key);

/**
 * 从Hash引擎中查找key对应的value，只可能有一个
 * @param engine HashEngine
 * @param key 要查找的key
 * @return 成功数目
 */
int32 putHashEngine(HashEngine *engine, uint32 keyLen, uint8 *key, uint32 valueLen, uint8 *value);

/**
 * 从Hash引擎中删除一个key
 * @param engine HashEngine
 * @param key 要查找的key
 * @return {Array} value 被删除的value
 */
Array removeHashEngine(HashEngine *engine, uint32 keyLen, uint8 *key);

/**
 * 持久化函数，在持久化线程执行
 * @param engine HashEngine
 */
void flushHashEngine(HashEngine *engine);


#ifdef PROFILE_TEST

#endif

#endif