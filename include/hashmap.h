/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * 一个桶大小固定的HashMap
 * 
 * 实现创建、删除、清空HashMap，支持插入查询
 * 
 * 内存管理方式为：
 * key自动管理：插入操作将创建key的副本，删除，清空，淘汰等操作会释放key的内存
 * value交由调用者管理：不会创建副本，直接赋值，或设为NULL
 * 
 * @filename: hashmap.h 
 * @description: HashMap结构与函数声明
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-12-08
 ******************************************************************************/
#pragma once
#ifndef __HASHMAP_H__
#define __HASHMAP_H__

#include "global.h"
#include "util.h"

/*****************************************************************************
 * 类型定义
 ******************************************************************************/

struct Entry; //去除警告用

typedef void* (*ForeachMapFunction)(struct Entry * entry, void* args);

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

/** LRU缓存定义 */
typedef struct HashMap
{
	/** hashtable桶数组的长度，值为2^n，且大于等于capacity/0.75 */
	uint32 bucketCapacity;
	/** 目前HashMap的尺寸 */
	uint32 size;
	/** hashtable的桶数组 */
	struct Entry **table;

} HashMap;

/** HashMap一个节点 */
typedef struct Entry
{
	/** 键长度 */
	uint32 keyLen;
	/** 键 */
	uint8* key;
	/** hashCode */
	uint32 hashCode;
	/** 值 */
	void* value;
	/** 单链表的后继 */
	struct Entry *after;
} Entry;

/*****************************************************************************
 * 公开API
 ******************************************************************************/

/**
 * 创建一个桶大小固定的HashMap
 * @param capacity 预估的最大容量，实际容量大于该容量，HashMap的性能将下降
 * @return {HashMap*} 一个可用HashMap，当不满足创建条件返回NULL
 */
HashMap *makeHashMap(uint32 capacity);

/**
 * 清空一个HashMap，并释放其占用内存
 */
void freeHashMap(HashMap* map);

/**
 * 遍历HashMap，允许free Entry，当func返回非NULL时停止并返回该值
 * @param map HashMap
 * @param func 执行函数
 * @param args 外部参数（代替闭包）
 * @return func的返回值或者NULL
 */
void* foreachHashMap(HashMap *map, ForeachMapFunction func, void* args);

/**
 * 清空一个HashMap
 * @param cache 待操作的LRU缓存对象
 */
void clearHashMap(HashMap *map);

/**
 * 向LRU缓存中插入或更新一条数据
 * key内存有缓存自动管理，即插入创建拷贝，删除淘汰释放内存
 * @param map 待操作的HashMap
 * @param keyLen 值的长度
 * @param key 键
 * @param value 值
 */
void putHashMap(HashMap *map, uint32 keyLen, uint8 *key, void *value);

/**
 * 从HashMap中获取key对应的value，若不存在返回NULL
 * @param map 待操作的HashMap
 * @param keyLen 值的长度
 * @param key 键
 * @return {void *} value字节数组或者NULL
 */
void *getHashMap(HashMap *map, uint32 keyLen, uint8 *key);

/**
 * 从HashMap中获取删除一对key
 * @param map 待操作的HashMap
 * @param keyLen 值的长度
 * @param key 键
 * @return {void *} 被移除的value字节数组或者NULL
 */
void *removeHashMap(HashMap *map, uint32 keyLen, uint8 *key);

/*****************************************************************************
 * 私有且需要测试或在测试中要使用的函数
 ******************************************************************************/
#ifdef PROFILE_TEST
/**
 * 创建一个HashMap节点，用来存放数据
 * @param keyLen 值的长度
 * @param key 键
 * @param value 值
 * @return {Entry *} 一个可用HashMap节点
 */
Entry *makeEntry(uint32 keyLen, uint8 *key, void *value, uint32 hashcode);
#endif


#endif