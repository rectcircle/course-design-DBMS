/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * 一个LRUCache的实现
 * 
 * 实现创建、删除、清空LRUCache，支持插入查询
 * 
 * 内存管理方式为：
 * key自动管理：插入操作将创建key的副本，删除，清空，淘汰等操作会释放key的内存
 * value交由调用者管理：不会创建副本，直接赋值，或设为NULL
 * 
 * 也就是说两个key值相同LRUNode，其key指向不同的内存，value指向同一内存（当然这是不可能出现的）
 * 
 * 该结构可以当做不支持扩容的HashMap使用（将LRUCache.capacity）设为max_int即可
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
	void* value;
	/** 双向链表的前驱指针 */
	struct LRUNode *prev;
	/** 双向链表的后继指针 */
	struct LRUNode *next;
	/** 单链表的后继指针 */
	struct LRUNode *after;
} LRUNode;

/*****************************************************************************
 * 公开API
 ******************************************************************************/

/**
 * 创建一个可用的LRU缓存
 * @param capacity LRU的容量
 * @param keyLen   key的字节数
 * @return {LRUCache*} 一个可用LRU缓存，当不满足创建条件返回NULL
 */
LRUCache *makeLRUCache(uint32 capacity, uint32 keyLen);

/**
 * 清空一个LRU，并释放其占用内存
 */
void freeLRUCache(LRUCache* cache);

/**
 * 向LRU缓存中插入或更新一条数据，若发生淘汰，将会free key和value的内存
 * key内存有缓存自动管理，即插入创建拷贝，删除淘汰释放内存
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @param value 值
 */
void* putLRUCache(LRUCache *cache, uint8 *key, void *value);

/**
 * 向LRU缓存中插入或更新一条数据
 * 当有数据要被淘汰时，将执行hook函数，hook的定义如下：
 * void hook(uint32 keyLen, uint8* key, void* value)
 * 在函数体内不会free淘汰者的内存
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @param value 值
 * @param hook 淘汰数据执行的hook
 * @return {void*} 返回被淘汰value的指针
 */
void* putLRUCacheWithHook(
	LRUCache *cache, 
	uint8 *key, 
	void *value, 
	void (*hook)(uint32, uint8*, void*));

/**
 * 从LRU缓存中获取key对应的value，若不存在返回NULL
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @return {int8 *} value字节数组或者NULL
 */
void *getLRUCache(LRUCache *cache, uint8 *key);

/**
 * 从LRU缓存中获取key对应的value，若不存在返回NULL，不将查到的节点移动到首部（可以并发进行遍历）
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @return {int8 *} value字节数组或者NULL
 */
void *getLRUCacheNoChange(LRUCache *cache, uint8 *key);

/**
 * 从LRU缓存中获取删除一对key
 * @param cache 待操作的LRU缓存对象
 * @param key 键
 * @return {int8 *} 被移除的value字节数组或者NULL
 */
void *removeLRUCache(LRUCache *cache, uint8 *key);

/**
 * 清空一个缓存
 * @param cache 待操作的LRU缓存对象
 */
void clearLRUCache(LRUCache *cache);

/*****************************************************************************
 * 私有且需要测试或在测试中要使用的函数
 ******************************************************************************/
#ifdef PROFILE_TEST
/**
 * 创建一个LRU节点，用来存放数据
 * @param key 键
 * @param value 值
 * @return {LRUNode} 一个可用LRU节点
 */
LRUNode *makeLRUNode(uint8 *key, void *value);
#endif


#endif