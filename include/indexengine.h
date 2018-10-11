/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: indexengine.h 
 * @description: 索引存储引擎API
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-08
 ******************************************************************************/
#pragma once
#ifndef __INDEXENGINE_H__
#define __INDEXENGINE_H__

#include "global.h"
#include "util.h"
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <arpa/inet.h>
#include <sys/stat.h>

/*****************************************************************************
 * 宏定义
 ******************************************************************************/

/** 
 * IndexEngine设置获取标志的宏
 */
/** 取标志isUnique的值，表示该索引的key是否唯一（不允许重复） */
#define IS_UNIQUE(flag) (flag & 1)
#define SET_UNIQUE(flag) (flag |=1)
#define CLR_UNIQUE(flag) (flag &=~1)
/** 取标志isPersistence的值，表示是否正在进行持久化 */
#define IS_PERSISTENCE(flag) ((flag>>1) & 1)
#define SET_PERSISTENCE(flag) (flag |= (1<<1))
#define CLR_PERSISTENCE(flag) (flag &= ~(1 << 1))
/** 取标志isSwitchTree的值，表示是否正在进行树持久化 */
#define IS_SWITCHTREE(flag) ((flag >> 2) & 1)
#define SET_SWITCHTREE(flag) (flag |= (1 << 2))
#define CLR_SWITCHTREE(flag) (flag &= ~(1 << 2))
/** 取标志isCreating的值，表示是否正在进行创建节点 */
#define IS_CREATING(flag) ((flag >> 3) & 1)
#define SET_CREATING(flag) (flag |= (1 << 3))
#define CLR_CREATING(flag) (flag &= ~(1 << 3))

/** 
 * IndexTreeNode和IndexTreLeaf取标志的宏
 */
/** 取标志isDeprecated的值，表示是否被废弃 */
#define IS_DEPRECATED(flag) (flag & 1)
#define SET_DEPRECATED(flag) (flag |= 1)
#define CLR_DEPRECATED(flag) (flag &= ~1)

/**
 * 数据页类型宏
 */
/** 非叶子节点页（链接节点） */
#define TYPE_LINK 1
/** 带元数据的叶子节点页 */
#define TYPE_LEAF_WITH_META 2
/** 不带元数据的叶子节点页 */
#define TYPE_LEAF_NO_META 3

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

/**
 * 索引引擎的B+树的元数据页，在文件第0页
 */
typedef struct IndexTreeMeta
{
	/**
	 * B+树的度、扇出、阶：
	 * 节点最多有degree个孩子
	 * 节点最多有degree个key
	 */
	uint32 degree;
	/** 每个键的字节数 */
	uint32 keyLen;
	/** 每个值的字节数 */
	uint32 valueLen;
	/** 目前树的深度 */
	uint32 depth;
	/** 是否唯一，是否允许出现重复的键 */
	int8 isUnique;
	/** B+树的根节点：可选类型为IndexTreeNode、IndexTreeLeaf */
	uint64 root;
	/** 最小的叶子节点，用于遍历 */
	uint64 sqt;
} IndexTreeMeta;

/**
 * 索引引擎
 */
typedef struct IndexEngine {
	/** 索引文件位置 */
	char* filename;
	/** 索引文件描述符 */
	int fd;
	/** magic魔数 */
	uint32 magic;
	/** 文件版本 */
	uint32 version;
	/** 页大小 */
	uint32 pageSize;
	/** 标志使用IS_XXX的宏获取具体标志 */
	uint32 flag;
	/** 下一个可用⻚的号 */
	uint64 nextPageId;
	/** [1, nextPageId) 已将使用的⻚的数目,结合 nextPageId 在达到一定情况下进行自动磁盘整理 */
	uint64 usedPageCnt;
	/** 该索引数据计数(共多少条数据) */
	uint64 count;
	/** B+树的元数据 */
	struct IndexTreeMeta treeMeta;
} IndexEngine;

/**
 * 索引引擎的一个数据页
 */
typedef struct IndexTreeNode
{
	/** 该节点在磁盘的页面号 */
	uint64 pageId;
	/** 若该页被修改，该字段有效*/
	uint64 newPageId;
	/** 该页的类型，类型参见TYPE_XXX宏 */
	uint8 type;
	/** 已经被使用的key的数目*/
	uint32 size;
	/** 标志使用IS_XXX的宏获取具体标志 */
	uint32 flag;
	/** 指向相邻节点的指针 */
	uint64 next;
	/** 若当前节点为废弃，此字段为指向有效节点的指针 */
	uint64 effect;
	/** 当前页为叶子节点页，指向下一个数据页所在的位置 */
	uint64 after;
	/** 
	 * key的数组
	 * 数组的长度为：(BTree.degree+1)，多余的一个空间用于节点分裂使用
	 * 每个元素的长度为：(keyLen)字节
	 */
	uint8 **keys;
	/** 
	 * 指向孩子的页的数组
	 * 数组的长度为：BTree.degree+1
	 */
	uint64 *children;
	/** 
	 * 指向孩子的页的数组
	 * 数组的长度为：BTree.degree+1
	 */
	uint8 **values;

} IndexTreeNode;

/*****************************************************************************
 * 公开API
 ******************************************************************************/

/*****************************************************************************
 * 增删改查
 ******************************************************************************/

/**
 * 从索引引擎中查找key对应的value，可能有多个
 * @param engine 合法的B+树
 * @param key 要查找的key
 * @return {uint8*} 带长度的数组类型
 */
Array *searchIndexEngine(IndexEngine *engine, uint8 *key);

/**
 * 向BTree添加添加一条记录
 * 注意：不会进行重复判断，直接插入
 * 若想抱枕KV严格不重复（同一对KV在树中唯一），请先使用update，若返回0再进行插入
 * @param config 合法的B+树
 * @param key 要插入的key
 * @param value 要插入的value
 * @return {int32} -1 插入失败，1 表示插入成功
 */
int32 insertIndexEngine(IndexEngine *engine, uint8 *key, uint8 *value);

/**
 * 从BTree中删除记录
 * @param config 合法的B+树
 * @param key 要删除的key
 * @param value 要删除的value可为NULL，为NULL表示删除所有满足key的记录
 * @return {int32} 表示删除的记录数目
 */
int32 removeIndexEngine(IndexEngine *engine, uint8 *key, uint8 *value);

/**
 * 更新KV严格相等的valeu为newValue
 * @param config 合法的B+树
 * @param key 要更新的key
 * @param oldValue 要更新的之前的value
 * @param newValue 新的value
 * @return {int32} 0 没有选中的数据，1 更新成功
 */
int32 updateIndexEngine(IndexEngine *engine, uint8 *key, uint8 *oldValue, uint8 *newValue);

/*****************************************************************************
 * 文件操作
 ******************************************************************************/

/**
 * 创建一个索引引擎，并创建文件，并打开文件，文件大小为2个页
 * 第一个页存放元数据，第二个页存放根节点
 * @param filename 文件路径
 * @param pageSize 页大小 0 表示默认16k，或>=104字节
 * @param isUnique 是否唯一
 * @param keyLen 键字节数
 * @param valueLen 值字节数
 * @return 一个可用的 索引引擎指针，参数异常，返回NULL
 */
IndexEngine *makeIndexEngine(
	char *filename,
	uint32 keyLen,
	uint32 valueLen,
	uint32 pageSize,
	int8 isUnique);

/**
 * 从文件系统加载一个索引引擎，文件不存在返回NULL
 * 会进行启动检查，恢复状态
 * @param filename 文件路径
 * @return 一个可用的 索引引擎指针，文件不存在返回NULL
 */
IndexEngine *loadIndexEngine(char *filename);

/*****************************************************************************
 * 辅助函数
 ******************************************************************************/

/**
 * 刷磁盘，持久化操作
 */
void flushIndexEngine(IndexEngine *engine);

/**
 * 文件碎片整理
 */
void defragmentationIndexEngine(IndexEngine *engine);

/**
 * 故障恢复
 */
void recoveryIndexEngine(IndexEngine);


/*****************************************************************************
 * 私有且需要测试或在测试中要使用的函数
 ******************************************************************************/
#ifdef PROFILE_TEST

/**
 * 创建一个文件-可读可写
 * @return fd 可读可写的文件描述符，文件存在或无法打开返回-1
 */
int createIndexFile(const char * filename);

/**
 * 打开文件-可读可写
 * @param filename 要创建的文件名
 * @return fd 可读可写的文件描述符，文件不存在或无法打开返回-1
 */
int openIndexFile(const char *filename);

/**
 * 将元数据填充到buffer（序列化）
 * @param engine IndexEngine
 * @param meta IndexTreeMeta
 * @param buffer 被填充的字节数组
 */
void metaToBuffer(IndexEngine* engine, char* buffer);

/**
 * 将节点数据填充到buffer（序列化）
 * @param engine IndexEngine
 * @param node IndexTreeNode
 * @param nodeType 可选值为TYPE_XXX宏
 * @param buffer 被填充的字节数组
 */
void nodeToBuffer(
	IndexEngine *engine, 
	IndexTreeNode *node, 
	int32 nodeType, 
	char *buffer);

/**
 * 将buffer解析成结构（反序列化）
 * @param engine IndexEngine
 * @param meta IndexTreeMeta
 * @param buffer 被填充的字节数组
 */
void bufferToMeta(IndexEngine *engine, char *buffer);

/**
 * 将buffer解析成结构（反序列化）
 * @param engine IndexEngine
 * @param node IndexTreeNode
 * @param nodeType 可选值为TYPE_XXX宏
 * @param buffer 被填充的字节数组
 */
void bufferToNode(
	IndexEngine *engine,
	IndexTreeNode *node,
	int32 nodeType,
	char *buffer);

/**
 * 写入某一页
 * @param engine IndexEngine
 * @param pageId 页号
 * @param buffer 缓冲
 * @param len 写的长度
 */
void writePageIndexFile(IndexEngine *engine, uint64 pageId, char *buffer, uint32 len);

/**
 * 读取入某一页
 * @param engine IndexEngine
 * @param pageId 页号
 * @param buffer 缓冲
 * @param len 读的长度
 */
void readPageIndexFile(IndexEngine* engine, uint64 pageId, char *buffer, uint32 len);

#endif

#endif