/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: lrucache.h 
 * @description: LRU缓存结构与函数声明
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-06
 ******************************************************************************/
#pragma once
#ifndef __LRUCACHE_H__
#define __LRUCACHE_H__

#include "global.h"
#include "util.h"

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

/** LRU缓存定义 */
typedef struct LRUCache
{
	/** 缓存最大容量，当缓存满了之后将会进行淘汰 */
	uint32 capacity;
	/** hashtable桶数组的长度，值为2^n，且大于等于capacity/0.75 */
	uint32 bucketCapacity;
	/** 目前缓存的尺寸 */
	uint32 size;
	/** 每个键的字节数 */
	uint32 keyLen;
	/** hashtable的桶数组 */
	struct LRUNode **table;
	/** 双向循环链表的头指针 */
	struct LRUNode *head;

} LRUCache;

/** LRU数据节点，同时是一个双向链表的节点和单链表的节点 */
typedef struct LRUNode
{
	/** 键 */
	uint8* key;
	/** 值 */
	uint8* value;
	/** 双向链表的前驱指针 */
	struct LRUNode *prev;
	/** 双向链表的后继指针 */
	struct LRUNode *next;
	/** 单链表的后继指针 */
	struct LRUNode *after;
} LRUNode;

/**
 * 创建一个可用的LRU缓存
 * @param capacity LRU的容量
 * @param keyLen   key的字节数
 * @return {LRUCache*} 一个可用LRU缓存，当不满足创建条件返回NULL
 */
LRUCache *makeLRUCache(uint32 capacity, uint32 keyLen);

/**
 * 创建一个LRU节点，用来存放数据
 * @param key 键
 * @param value 值
 * @return {LRUNode} 一个可用LRU节点
 */
LRUNode *makeLRUNode(uint8 *key, uint8* value);

/**
 * 向LRU缓存中插入或更新一条数据
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @param value 值
 */
void putLRUCache(LRUCache *cache, uint8 *key, uint8 *value);

/**
 * 向LRU缓存中插入或更新一条数据
 * 当有数据要被淘汰时，将执行hook函数，hook的定义如下：
 * void hook(uint32 keyLen, uint8* key, uint8* value)
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @param value 值
 * @param hook 淘汰数据执行的hook
 */
void putLRUCacheWithHook(
	LRUCache *cache, 
	uint8 *key, 
	uint8 *value, 
	void (*hook)(uint32, uint8*, uint8*));

/**
 * 从LRU缓存中获取key对应的value，若不存在返回NULL
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @return {int8 *} value字节数组或者NULL
 */
uint8 *getLRUCache(LRUCache *cache, uint8 *key);

#endif