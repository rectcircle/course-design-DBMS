/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: lrucache.c 
 * @description: LRU缓存函数实现
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-06
 ******************************************************************************/
#include <malloc.h>
#include "lrucache.h"

/*****************************************************************************
 * 通用私有辅助函数
 ******************************************************************************/

/** 计算key的hash值 */
static uint32 hashCode(uint8 *key, uint32 keyLen){
	//改进的32位FNV算法1
	static uint32 p = 16777619;
	uint32 hash = 2166136261;
	for (int i = 0; i < keyLen; i++)
		hash = (hash ^ key[i]) * p;
	hash += hash << 13;
	hash ^= hash >> 7;
	hash += hash << 3;
	hash ^= hash >> 17;
	hash += hash << 5;
	return hash;
}

/** 从HashTable中查找节点 */
static LRUNode* getFromHashTable(LRUCache* cache, uint8* key, uint32 hashcode){
	uint32 index = hashcode & (cache->bucketCapacity-1);
	LRUNode* p = cache->table[index];
	while(p){
		if(byteArrayCompare(cache->keyLen, key, p->key)==0){
			return p;
		}
		p = p->after;
	}
	return NULL;
}

/** 直接插入到HashTable中，不考虑重复 */
static void insertToHashTable(LRUCache* cache, LRUNode* node, uint32 hashcode){
	uint32 index = hashcode & (cache->bucketCapacity - 1);
	LRUNode *p = cache->table[index];
	node->after = p;
	cache->table[index] = node;
}

/** 从HashTable中删除一个节点并返回，不会释放内存 */
static LRUNode *removeFromHashTable(LRUCache *cache, uint8 *key, uint32 hashcode){
	uint32 index = hashcode & (cache->bucketCapacity - 1);
	LRUNode* p = cache->table[index];
	//p第一个位置
	if(p!=NULL && byteArrayCompare(cache->keyLen, key, p->key)==0){
		cache->table[index] = p->after;
		return p;
	}
	//p是q的前驱
	LRUNode* q = p->after;
	while(q){
		if(byteArrayCompare(cache->keyLen, key, q->key)==0){
			p->after = q->after;
			return q;
		}
		p = q;
		q = q->after;
	}
	return NULL;
}

/** 从带头结点的双向循环链表中删除node节点：不会释放节点内存 */
static void removeLRUNode(LRUNode *node){
	node->prev->next = node->next;
	node->next->prev = node->prev;
}

/** 向带头结点的双向循环链表头插入元素 */
static void insertLRUNode(LRUNode *head, LRUNode *node){
	node->next = head->next;
	node->prev = head;
	head->next->prev = node;
	head->next = node;
}

/** 将节点搬移到循环队列的首部 */
static void moveToFirst(LRUCache* cache, LRUNode* node){
	removeLRUNode(node);
	insertLRUNode(cache->head, node);
}


/*****************************************************************************
 * 公有函数
 ******************************************************************************/
LRUCache *makeLRUCache(uint32 capacity, uint32 keyLen){
	//hash表的负载因子控制在0.75
	static double loadFactor = 0.75;
	//无法达到0.75的负载因子
	if(capacity>0xffffffffu*loadFactor){
		return NULL;
	}

	LRUCache* cache = (LRUCache*)malloc(sizeof(LRUCache));
	//缓存容量初始化
	cache->capacity = capacity;
	//计算并初始化桶数组容量
	capacity = (uint32)(((double)capacity)/loadFactor);
	uint32 bucketCapacity = 1;
	while (bucketCapacity < capacity)
		bucketCapacity <<= 1;
	cache->bucketCapacity = bucketCapacity;
	//其他值初始化
	cache->size = 0;
	cache->keyLen = keyLen;
	cache->table = (LRUNode**)malloc(sizeof(LRUNode*)*bucketCapacity);
	//为了方便编程，创建一个头结点
	cache->head = makeLRUNode(NULL, NULL);
	cache->head->prev = cache->head;
	cache->head->next = cache->head;
	return cache;
}

LRUNode *makeLRUNode(uint8 *key, uint8 *value){
	LRUNode *node = (LRUNode *)malloc(sizeof(LRUNode));
	node->key = key;
	node->value = value;
	node->prev = NULL;
	node->next = NULL;
	node->after = NULL;
	return node;
}

void putLRUCache(LRUCache *cache, uint8 *key, uint8 *value){
	putLRUCacheWithHook(cache, key, value, NULL);
}

void putLRUCacheWithHook(
LRUCache *cache,
uint8 *key,
uint8 *value,
void (*hook)(uint32, uint8 *, uint8 *)){
	uint32 hashcode = hashCode(key, cache->keyLen);
	LRUNode* node = getFromHashTable(cache, key, hashcode);
	if(node!=NULL){
		free(node->value);
		node->value=value;
		moveToFirst(cache, node);
	} else {
		if(cache->size>=cache->capacity){
			//从hash表中删除最后一个元素
			node = removeFromHashTable(
				cache,
				cache->head->prev->key,
				hashCode(cache->head->prev->key, cache->keyLen));
			//从双向链表中删除节点
			removeLRUNode(node);
			//复用node节点
			//调用hook
			if(hook!=NULL) hook(cache->keyLen, node->key, node->value);
			//清理内存
			free(node->key); 
			free(node->value);
			node->key = key;
			node->value = value;
		} else {
			node = makeLRUNode(key,value);
			cache->size++;
		}
		//插到链表首部
		insertLRUNode(cache->head, node);
		//插到HashMap结构中
		insertToHashTable(cache, node, hashcode);
	}
}

uint8 *getLRUCache(LRUCache *cache, uint8 *key){
	uint32 hashcode = hashCode(key, cache->keyLen);
	LRUNode* node = getFromHashTable(cache, key, hashcode);
	if(node!=NULL){
		moveToFirst(cache, node);
		return node->value;
	}
	return NULL;
}
