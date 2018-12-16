/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: hashmap.c 
 * @description: hashmap函数实现
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-12-08
 ******************************************************************************/
#include <malloc.h>
#include "hashmap.h"

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

private Entry *makeEntry(uint32 keyLen, uint8 *key, void *value, uint32 hashcode){
	Entry *entry = (Entry *)calloc(1, sizeof(Entry));
	entry->keyLen = keyLen;
	entry->key = key;
	entry->hashCode = hashcode;
	entry->value = value;
	return entry;
}

private void freeEntry(Entry* entry){
	free(entry->key);
	free(entry);
}

/** 从HashMap中查找节点 */
private Entry* searchEntry(HashMap* map, uint32 keyLen, uint8* key, uint32 hashcode){
	uint32 index = hashcode & (map->bucketCapacity-1);
	Entry *p = map->table[index];
	while(p){
		if(p->keyLen == keyLen && p->hashCode == hashcode && byteArrayCompare(keyLen, key, p->key)==0){
			return p;
		}
		p = p->after;
	}
	return NULL;
}

/** 直接插入到HashMap中，不考虑重复 */
private void insertEntry(HashMap* map, Entry* entry, uint32 hashcode){
	uint32 index = hashcode & (map->bucketCapacity - 1);
	Entry *p = map->table[index];
	entry->after = p;
	map->table[index] = entry;
}

/** 从HashTable中删除一个节点并返回，不会释放内存 */
private Entry *removeEntry(HashMap *map, uint32 keyLen, uint8 *key, uint32 hashcode){
	uint32 index = hashcode & (map->bucketCapacity - 1);
	Entry* p = map->table[index];
	if(p==NULL){
		return NULL;
	}
	//p第一个位置
	if(p->keyLen == keyLen && p->hashCode == hashcode && byteArrayCompare(keyLen, key, p->key)==0){
		map->table[index] = p->after;
		return p;
	}
	//p是q的前驱
	Entry* q = p->after;
	while(q){
		if(q->keyLen == keyLen && q->hashCode == hashcode && byteArrayCompare(keyLen, key, q->key)==0){
			p->after = q->after;
			return q;
		}
		p = q;
		q = q->after;
	}
	return NULL;
}


/*****************************************************************************
 * 公有函数
 ******************************************************************************/
HashMap *makeHashMap(uint32 capacity){
	//hash表的负载因子控制在0.75
	static double loadFactor = 0.75;
	//无法达到0.75的负载因子
	if(capacity>0xffffffffu*loadFactor){
		return NULL;
	}

	HashMap *map = (HashMap *)malloc(sizeof(HashMap));
	//计算并初始化桶数组容量
	capacity = (uint32)(((double)capacity)/loadFactor);
	uint32 bucketCapacity = 1;
	while (bucketCapacity < capacity)
		bucketCapacity <<= 1;
	map->bucketCapacity = bucketCapacity;
	//其他值初始化
	map->size = 0;
	map->table = (Entry **)calloc(bucketCapacity, sizeof(Entry *));
	return map;
}

void freeHashMap(HashMap* map){
	if(map==NULL){
		return;
	}
	clearHashMap(map);
	free(map->table);
	free(map);
}

void* foreachHashMap(HashMap *map, ForeachMapFunction func, void *args){
	for (int i = 0; i < map->bucketCapacity; i++){
		Entry *p = map->table[i], *q=NULL;
		if (p==NULL){
			continue;
		}
		q = p->after;
		while(p!=NULL) {
			void* result = func(p, args);
			if(result != NULL){
				return result;
			}
			p = q;
			if(q!=NULL){
				q = q->after;
			}
		}
	}
	return NULL;
}

static void* foreachFreeEntry(Entry* entry, void* args){
	freeEntry(entry);
	return NULL;
}

void clearHashMap(HashMap *map){
	foreachHashMap(map, (ForeachMapFunction) foreachFreeEntry, NULL);
	//table清零
	memset(map->table, 0, map->bucketCapacity * sizeof(Entry *));
	map->size=0;
}

void putHashMap(HashMap *map, uint32 keyLen, uint8 *originKey, void *value){
	uint32 hashcode = hashCode(originKey, keyLen);
	Entry *node = searchEntry(map, keyLen, originKey, hashcode);
	if(node!=NULL){
		node->value=value;
		return;
	}
	uint8 *key = (uint8 *)malloc(keyLen);
	memcpy(key, originKey, keyLen);
	node = makeEntry(keyLen, key, value, hashcode);
	map->size++;
	insertEntry(map, node, hashcode);
}


void *getHashMap(HashMap *map, uint32 keyLen, uint8 *key){
	uint32 hashcode = hashCode(key, keyLen);
	Entry *node = searchEntry(map, keyLen, key, hashcode);
	if(node!=NULL){
		return node->value;
	}
	return NULL;
}

void *removeHashMap(HashMap *map, uint32 keyLen, uint8 *key){
	uint32 hashcode = hashCode(key, keyLen);
	Entry *node = removeEntry(
		map,
		keyLen,
		key,
		hashcode);
	if(node==NULL){
		return NULL;
	}
	void *result = node->value;
	freeEntry(node);
	map->size--;
	return result;
}