/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: indexengine.cpp
 * @description: 索引存储引擎API
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-09
 ******************************************************************************/

#include "indexengine.h"

//魔数
static const uint32 MAGIC_NUMBER=0x960729dbu;
//索引元数据长度
static const uint32 INDEX_META_SIZE = 136;
//索引元数据长度，不带备份
static const uint32 INDEX_META_SIZE_NO_BACK = 80;
//节点元数据长度
static const uint32 NODE_META_SIZE = 40;


static uint64 getNextPageId(IndexEngine* engine){
	return engine->nextPageId++;
}

/*****************************************************************************
 * 私有函数：申请释结构放内存，结构状态变化
 ******************************************************************************/

/** 创建一个Node，用于存放数据 */
private IndexTreeNode* makeIndexTreeNode(IndexEngine* engine, int32 nodeType){
	IndexTreeNode *node = (IndexTreeNode *)calloc(1,sizeof(IndexTreeNode));
	node->keys = (uint8 **)malloc(sizeof(uint8 *) * (engine->treeMeta.degree + 1));
	node->type = nodeType;
	if (nodeType == NODE_TYPE_LINK){
		node->children = (uint64 *)malloc(sizeof(uint64) * (engine->treeMeta.degree + 1));
	} else {
		node->values = (uint8 **)malloc(sizeof(uint8 *) * (engine->treeMeta.degree + 1));
	}
	return node;
}

/** 
 * 改变节点状态 参见《3-索引存储引擎》状态转换图
 * 如果从OLD转换为UPDATE，更新元数据
 * @param engine IndexEngine
 * @param node 若node为null，创建一个NEW状态node状态的节点，否则转换为nodeType状态
 * @param nodeType 节点要变为的状态
 * @param 转换状态之后的节点
 */
private IndexTreeNode* changeIndexTreeNodeStatus(IndexEngine* engine, IndexTreeNode* node, int32 nodeType){
	if(node==NULL){
		IndexTreeNode *result = makeIndexTreeNode(engine, nodeType);
		result->status = NODE_STATUS_NEW;
		result->type = nodeType;
		result->nodeVersion = engine->nextNodeVersion;
		result->newPageId = getNextPageId(engine);
		result->pageId = result->newPageId;
		return result;
	}
	if(node->status==NODE_STATUS_OLD){
		if(nodeType==NODE_STATUS_UPDATE){
			node->status = NODE_STATUS_UPDATE;
			if(node->after==0){
				node->newPageId = getNextPageId(engine);
				node->after = node->newPageId;
			}
			node->nodeVersion = engine->nextNodeVersion;
		} else if(nodeType==NODE_STATUS_REMOVE){
			node->status = NODE_STATUS_REMOVE;
		} else {
			return NULL;
		}
	} else {
		return NULL;
	}
	return NULL;
}

/** 创建一个状态为NEW的节点，并分配一个PageId */
private IndexTreeNode* newIndexTreeNode(IndexEngine* engine, int32 nodeType){
	  return changeIndexTreeNodeStatus(engine, NULL, nodeType);
}

/** Free一个Node */
private void freeIndexTreeNode(IndexTreeNode* node, int32 nodeType){
	for(int i=0; i<node->size; i++){
		free(node->keys[i]);
		if (nodeType != NODE_TYPE_LINK){
			free(node->values[i]);
		}
	}
	free(node->keys);
	if (nodeType == NODE_TYPE_LINK){
		free(node->children);
	} else {
		free(node->values);
	}
	free(node);
}

/** 创建一个深拷贝的Node */
private IndexTreeNode* copyIndexTreeNode(IndexEngine* engine, IndexTreeNode* src){
	IndexTreeNode *dest = makeIndexTreeNode(engine, src->type);
	dest->pageId = src->pageId;
	dest->newPageId = src->newPageId;
	dest->type = src->type;
	dest->size = src->size;
	dest->flag = src->flag;
	dest->next = src->next;
	dest->nodeVersion = src->nodeVersion;
	dest->status = src->status;
	dest->after = src->after;

	if(src->type==NODE_TYPE_LINK){
		for (int i = 0; i < src->size; i++){
			dest->keys[i] = (uint8*)malloc(sizeof(uint8)*engine->treeMeta.keyLen);
			memcpy(dest->keys[i], src->keys[i], engine->treeMeta.keyLen);
			dest->children[i] = src->children[i];
		}
	} else {
		for (int i = 0; i < src->size; i++){
			dest->keys[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.keyLen);
			memcpy(dest->keys[i], src->keys[i], engine->treeMeta.keyLen);
			dest->values[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.valueLen);
			memcpy(dest->values[i], src->values[i], engine->treeMeta.valueLen);
		}
	}
	return dest;
}

// /** 一个赋值函数，将effect数据复制到指定位置 */
// static void changeFromEffectNode(IndexEngine* engine, IndexTreeNode *dest, IndexTreeNode* src){
// 	// 	dest->pageId = src->pageId;
// 	// 	dest->newPageId = src->newPageId;
// 	dest->type = src->type;
// 	dest->size = src->size;
// 	dest->flag = src->flag;
// 	dest->next = src->next;
// 	dest->effect = 0;
// 	dest->nodeVersion = src->nodeVersion;
// 	dest->status = NODE_STATUS_UPDATE;

// 	//拷贝数据
// 	if(src->type==NODE_TYPE_LINK){
// 		for (int i = 0; i < src->size; i++){
// 			free(dest->keys[i]);
// 			dest->keys[i] = (uint8*)malloc(sizeof(uint8)*engine->treeMeta.keyLen);
// 			memcpy(dest->keys[i], src->keys[i], engine->treeMeta.keyLen);
// 			dest->children[i] = src->children[i];
// 		}
// 	} else {
// 		for (int i = 0; i < src->size; i++){
// 			free(dest->keys[i]);
// 			dest->keys[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.keyLen);
// 			memcpy(dest->keys[i], src->keys[i], engine->treeMeta.keyLen);
// 			free(dest->values[i]);
// 			dest->values[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.valueLen);
// 			memcpy(dest->values[i], src->values[i], engine->treeMeta.valueLen);
// 		}
// 	}
// }

/*****************************************************************************
 * 私有函数：文件操作、序列化、反序列化函数
 ******************************************************************************/

private int createIndexFile(const char * filename){
	return open(filename, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

private int openIndexFile(const char * filename){
	return open(filename, O_RDWR);
}

/** 将data拷贝到buffer，会转换为大端（网络字节序）方式 */
static int copyToBuffer(char* buffer, const void* data, uint32 len){
	uint16 dest2;
	uint32 dest4;
	uint64 dest8;
	switch (len)
	{
		case 2:
			dest2 = htons(*(uint16_t *)data);
			memcpy(buffer, &dest2, len);
			break;
		case 4:
			dest4 = htonl(*(uint32 *)data);
			memcpy(buffer, &dest4, len);
			break;
		case 8:
			dest8 = htonll(*(uint64*)data);
			memcpy(buffer, &dest8, len);
			break;
		default:
			memcpy(buffer, data, len);
			break;
	}
	return len;
}

private void metaToBuffer(IndexEngine* engine, char* buffer){
	IndexTreeMeta* meta = &engine->treeMeta;
	int len = 0;
	len += copyToBuffer(buffer+len, &engine->magic, sizeof(engine->magic));
	len += copyToBuffer(buffer + len, &engine->version, sizeof(engine->version));
	len += copyToBuffer(buffer + len, &engine->pageSize, sizeof(engine->magic));
	len += copyToBuffer(buffer + len, &engine->flag, sizeof(engine->flag));
	len += copyToBuffer(buffer + len, &meta->degree, sizeof(meta->degree));
	len += copyToBuffer(buffer + len, &meta->depth, sizeof(meta->depth));
	len += copyToBuffer(buffer + len, &meta->keyLen, sizeof(meta->keyLen));
	len += copyToBuffer(buffer + len, &meta->valueLen, sizeof(meta->valueLen));
	len += copyToBuffer(buffer + len, &engine->count, sizeof(engine->count));
	len += copyToBuffer(buffer + len, &meta->root, sizeof(meta->root));
	len += copyToBuffer(buffer + len, &meta->sqt, sizeof(meta->sqt));
	len += copyToBuffer(buffer + len, &engine->nextPageId, sizeof(engine->nextPageId));
	len += copyToBuffer(buffer + len, &engine->usedPageCnt, sizeof(engine->usedPageCnt));
	len += copyToBuffer(buffer + len, &engine->nextNodeVersion, sizeof(engine->nextNodeVersion));
}

private void nodeToBuffer(IndexEngine* engine, IndexTreeNode *node, int32 nodeType, char* buffer){
	int len = 0;
	uint64 after = 0;
	if(node->after!=0&&node->pageId==node->newPageId){
		after = node->after;
	}
	len += copyToBuffer(buffer + len, &node->prev, sizeof(node->prev));
	len += copyToBuffer(buffer + len, &node->next, sizeof(node->next));
	len += copyToBuffer(buffer + len, &after, sizeof(node->after));
	len += copyToBuffer(buffer + len, &node->nodeVersion, sizeof(node->nodeVersion));
	len += copyToBuffer(buffer + len, &node->size, sizeof(node->size));
	len += copyToBuffer(buffer + len, &node->flag, sizeof(node->flag));
	uint32 keyLen = engine->treeMeta.keyLen;
	uint32 valueLen = engine->treeMeta.valueLen;

	if(nodeType==NODE_TYPE_LINK){
		for (int i = 0; i < node->size; i++){
			memcpy(buffer + len, node->keys[i], keyLen);
			len += keyLen;
			len += copyToBuffer(buffer + len, &node->children[i], sizeof(node->children[i]));
		}
	} else {
		for (int i = 0; i < node->size; i++){
			memcpy(buffer + len, node->keys[i], keyLen);
			len += keyLen;
			memcpy(buffer + len, node->values[i], valueLen);
			len += valueLen;
		}
	}
}


/** 从buffer中读取数据（网络字节序），转换为主机字节序，并赋值为data */
static int parseFromBuffer(char* buffer, const void* data, uint32 len){
	uint16 *dest2 = (uint16 *)data;
	uint32 *dest4 = (uint32 *)data;
	uint64 *dest8 = (uint64 *)data;
	switch (len)
	{
		case 2:
			*dest2 = ntohs(*(uint16_t *)buffer);
			break;
		case 4:
			*dest4 = htonl(*(uint32 *)buffer);
			break;
		case 8:
			*dest8 = htonll(*(uint64 *)buffer);
			break;
		default:
			break;
	}
	return len;
}

private void bufferToMeta(IndexEngine* engine, char* buffer){
	IndexTreeMeta* meta = &engine->treeMeta;
	int len = 0;
	len += parseFromBuffer(buffer+len, &engine->magic, sizeof(engine->magic));
	len += parseFromBuffer(buffer + len, &engine->version, sizeof(engine->version));
	len += parseFromBuffer(buffer + len, &engine->pageSize, sizeof(engine->magic));
	len += parseFromBuffer(buffer + len, &engine->flag, sizeof(engine->flag));
	len += parseFromBuffer(buffer + len, &meta->degree, sizeof(meta->degree));
	len += parseFromBuffer(buffer + len, &meta->depth, sizeof(meta->depth));
	len += parseFromBuffer(buffer + len, &meta->keyLen, sizeof(meta->keyLen));
	len += parseFromBuffer(buffer + len, &meta->valueLen, sizeof(meta->valueLen));
	len += parseFromBuffer(buffer + len, &engine->count, sizeof(engine->count));
	len += parseFromBuffer(buffer + len, &meta->root, sizeof(meta->root));
	len += parseFromBuffer(buffer + len, &meta->sqt, sizeof(meta->sqt));
	len += parseFromBuffer(buffer + len, &engine->nextPageId, sizeof(engine->nextPageId));
	len += parseFromBuffer(buffer + len, &engine->usedPageCnt, sizeof(engine->usedPageCnt));
	len += parseFromBuffer(buffer + len, &engine->nextNodeVersion, sizeof(engine->nextNodeVersion));
}

private void bufferToNode(
	IndexEngine *engine,
	IndexTreeNode *node,
	int32 nodeType,
	char *buffer){

	node->status = NODE_STATUS_OLD;
	node->type = nodeType;
	int len = 0;

	len += parseFromBuffer(buffer + len, &node->prev, sizeof(node->prev));
	len += parseFromBuffer(buffer + len, &node->next, sizeof(node->next));
	len += parseFromBuffer(buffer + len, &node->after, sizeof(node->after));
	len += parseFromBuffer(buffer + len, &node->nodeVersion, sizeof(node->nodeVersion));
	len += parseFromBuffer(buffer + len, &node->size, sizeof(node->size));
	len += parseFromBuffer(buffer + len, &node->flag, sizeof(node->flag));

	uint32 keyLen = engine->treeMeta.keyLen;
	uint32 valueLen = engine->treeMeta.valueLen;
	if(nodeType==NODE_TYPE_LINK){
		for (int i = 0; i < node->size; i++){
			node->keys[i] = (uint8*)malloc(sizeof(uint8)*engine->treeMeta.keyLen);
			memcpy(node->keys[i], buffer + len, keyLen);
			len += keyLen;
			len += parseFromBuffer(buffer + len, &node->children[i], sizeof(node->children[i]));
		}
	} else {
		for (int i = 0; i < node->size; i++){
			node->keys[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.keyLen);
			memcpy(node->keys[i], buffer + len, keyLen);
			len += keyLen;
			node->values[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.valueLen);
			memcpy(node->values[i], buffer + len, valueLen);
			len += valueLen;
		}
	}
}

private uint64 writePageIndexFile(IndexEngine* engine, uint64 pageId, char *buffer, uint32 len){
	lseek(engine->wfd, pageId*engine->pageSize, SEEK_SET);
	return write(engine->wfd, buffer, len);
}

private uint64 readPageIndexFile(IndexEngine* engine, uint64 pageId, char *buffer, uint32 len){
	lseek(engine->rfd, pageId * engine->pageSize, SEEK_SET);
	return read(engine->rfd, buffer, len);
}

private uint32 writeTypePosition(IndexEngine *engine, uint64 position, void *dest, uint32 len){
	char* buffer = (char*)malloc(len);
	copyToBuffer(buffer, dest, len);
	lseek(engine->wfd, position, SEEK_SET);
	write(engine->wfd, buffer, len);
	free(buffer);
	return len;
}

private uint32 readTypePosition(IndexEngine *engine, uint64 position, void *dest, uint32 len){
	char *buffer = (char *)malloc(len);
	lseek(engine->rfd, position, SEEK_SET);
	read(engine->rfd, buffer, len);
	parseFromBuffer(buffer, dest, len);
	free(buffer);
	return len;
}

uint32 writeArrayPosition(IndexEngine *engine, uint64 position, char *dest, uint32 len){
	lseek(engine->wfd, position, SEEK_SET);
	write(engine->wfd, dest, len);
	return len;
}

uint32 readArrayPosition(IndexEngine *engine, uint64 position, char *dest, uint32 len){
	lseek(engine->rfd, position, SEEK_SET);
	read(engine->rfd, dest, len);
	return len;
}

private IndexEngine *readMetaBackData(IndexEngine *engine){
	IndexEngine *engineBack = (IndexEngine *)malloc(sizeof(IndexEngine));
	uint32 len = 80;
	len += readTypePosition(engine, len, &engine->flag, sizeof(engine->flag));
	len += readTypePosition(engine, len, &engine->treeMeta.depth, sizeof(engine->treeMeta.depth));
	len += readTypePosition(engine, len, &engine->count, sizeof(engine->count));
	len += readTypePosition(engine, len, &engine->treeMeta.root, sizeof(engine->treeMeta.root));
	len += readTypePosition(engine, len, &engine->treeMeta.sqt, sizeof(engine->treeMeta.sqt));
	len += readTypePosition(engine, len, &engine->nextPageId, sizeof(engine->nextPageId));
	len += readTypePosition(engine, len, &engine->usedPageCnt, sizeof(engine->usedPageCnt));
	len += readTypePosition(engine, len, &engine->nextNodeVersion, sizeof(engine->nextNodeVersion));
	return engineBack;
}

private void writeMetaBackData(IndexEngine *engine){
	char metaBuffer[INDEX_META_SIZE];
	readPageIndexFile(engine, 0, metaBuffer, INDEX_META_SIZE);
	//写flag到flagBak
	uint32 len = 80;
	len += writeArrayPosition(engine, len, metaBuffer + 12, 4);
	len += writeArrayPosition(engine, len, metaBuffer + 20, 4);
	len += writeArrayPosition(engine, len, metaBuffer + 32, 6*8);
}

/*****************************************************************************
 * 私有函数：缓存组件操作
 ******************************************************************************/

/** 根据最大堆内存大小，创建引擎需要用到的缓存,返回0表示成功 */
static int32 initIndexCache(IndexEngine* engine, uint64 maxHeapSize){
	if(maxHeapSize/3 <= 32){
		return -1;
	}
	uint32 capacity = (maxHeapSize/3 - 32) / (32/3+56+engine->treeMeta.keyLen+engine->treeMeta.valueLen);
	if(capacity<3){
		return -1;
	}
	engine->cache.unchangeCache = makeLRUCache(capacity, sizeof(engine->treeMeta.root));
	engine->cache.changeCacheWork = makeLRUCache(capacity, sizeof(engine->treeMeta.root));
	engine->cache.changeCacheFreeze = makeLRUCache(capacity, sizeof(engine->treeMeta.root));
	//一个将changeCache的淘汰上线设置为无限
	engine->cache.changeCacheWork->capacity = MAX_UINT32;
	engine->cache.changeCacheFreeze->capacity = MAX_UINT32;
	engine->cache.status = 0;
	engine->cache.statusCond = malloc(sizeof(*engine->cache.statusCond));
	engine->cache.statusAttr = malloc(sizeof(*engine->cache.statusAttr));
	engine->cache.statusMutex = malloc(sizeof(*engine->cache.statusMutex));
	engine->cache.persistenceThread = malloc(sizeof(*engine->cache.persistenceThread));
	pthread_cond_init(engine->cache.statusCond, NULL);
	pthread_mutexattr_init(engine->cache.statusAttr);
	pthread_mutexattr_settype(engine->cache.statusAttr, PTHREAD_MUTEX_RECURSIVE_NP);
	pthread_mutex_init(engine->cache.statusMutex, engine->cache.statusAttr);
	return 0;
}

/** 切换change缓存 */
static void swapChangeCache(IndexEngine *engine){
	LRUCache* tmp = engine->cache.changeCacheFreeze;
	engine->cache.changeCacheFreeze = engine->cache.changeCacheWork;
	engine->cache.changeCacheWork = tmp;
}

/** 添加到changeCacheWork中（若UnchangeCache存在则删除） */
static void putTochangeCacheWork(IndexEngine* engine, IndexTreeNode* node){
	removeLRUCache(engine->cache.unchangeCache, (uint8 *)&node->pageId);
	putLRUCache(engine->cache.changeCacheWork, (uint8 *)&node->pageId, (void *)node);
}

/** 检查进行持久化 */
static void checkThreadPersistence(IndexEngine* engine){
	//changeCacheWork满了
	if(engine->cache.changeCacheWork->size>=engine->cache.unchangeCache->capacity){
		//读取缓存状态
		pthread_cleanup_push((void*)pthread_mutex_unlock, engine->cache.statusMutex);
		pthread_mutex_lock(engine->cache.statusMutex);
		//当其他线程正在进行持久化，等待
		while(engine->cache.status!=CACHE_STATUS_NORMAL){
			pthread_cond_wait(engine->cache.statusCond, engine->cache.statusMutex);
		}
		//进行cache切换
		swapChangeCache(engine);
		//将状态切换为持久化
		engine->cache.status = CACHE_STATUS_PERSISTENCE;
		//此时版本号增加
		engine->nextNodeVersion++;
		//不需要进一步检查如下if条件，因为其他线程不会修改条件
		//engine->cache.changeCacheWork->size>=engine->cache.unchangeCache->capacity
		pthread_mutex_unlock(engine->cache.statusMutex);
		pthread_cleanup_pop(0);

		//创建一个engine的备份，浅拷贝即可
		IndexEngine *freezeEngine = (IndexEngine *)malloc(sizeof(IndexEngine));
		memcpy(freezeEngine, engine, sizeof(IndexEngine));
		IndexEngine **threadArgs = (IndexEngine**) malloc(sizeof(IndexEngine*)*2);
		threadArgs[0] = engine;
		threadArgs[1] = freezeEngine;
		pthread_create(engine->cache.persistenceThread, NULL, (void *)flushIndexEngine, (void *)threadArgs);
		// flushIndexEngine(threadArgs);
	}
}

private IndexTreeNode* getTreeNodeByPageId(IndexEngine *engine, uint64 pageId, int32 nodeType){
	if(pageId==0){
		return NULL;
	}

	LRUCache *changeCacheWork = engine->cache.changeCacheWork;
	LRUCache *changeCacheFreeze = engine->cache.changeCacheFreeze;
	LRUCache *unchangeCache = engine->cache.unchangeCache;
	
	//首先从changeCacheWork中获取
	IndexTreeNode *result = getLRUCache(changeCacheWork, (uint8 *)&pageId);
	if(result!=NULL){
		//可以获取到直接返回
		return result;
	}

	//否则判断是否需要在changeCacheFreeze中获取
	pthread_mutex_lock(engine->cache.statusMutex);
	//当前处于持久化中的状态，搜索冻结的缓存
	if(engine->cache.status==CACHE_STATUS_PERSISTENCE){
		result = getLRUCache(changeCacheFreeze, (uint8*)&pageId);
	}
	//获取到了，将其拷贝插入到unchangeCache
	if (result != NULL){
		IndexTreeNode *copyNode = getLRUCache(unchangeCache, (uint8 *)&result->pageId);
		if(copyNode==NULL){
			copyNode = copyIndexTreeNode(engine, result);
			if(result->type==NODE_STATUS_NEW){
				copyNode->pageId = result->pageId;
				copyNode->newPageId = result->pageId;
			} else if(result->type==NODE_STATUS_UPDATE) {
				if(result->pageId==result->newPageId){
					copyNode->pageId = result->pageId;
					copyNode->newPageId = result->after;
				} else {
					copyNode->pageId = result->pageId;
					copyNode->newPageId = result->pageId;
				}
			}
			//状态为为old
			copyNode->status = NODE_STATUS_OLD;
			putLRUCache(unchangeCache, (uint8*)&copyNode->pageId, copyNode);
		}
		result = copyNode;
	}
	pthread_mutex_unlock(engine->cache.statusMutex);
	if (result != NULL){
		return result;
	}

	//从unchangeCache中读取
	result = getLRUCache(unchangeCache, (uint8*)&pageId);
	if(result!=NULL){
		return result;
	}

	//从文件中读取
	char *buffer = (char *)malloc(sizeof(char) * engine->pageSize);
	IndexTreeNode *nodes[2]={NULL, NULL};
	uint64 pageIdBak = pageId;
	for(int i=0; i<2 && pageId; i++){
		readPageIndexFile(engine, pageId, buffer, engine->pageSize);
		nodes[i] = makeIndexTreeNode(engine, nodeType);
		bufferToNode(engine, nodes[i], nodeType, buffer);
		IndexTreeNode *eliminateNode = (IndexTreeNode *)putLRUCache(unchangeCache, (uint8*)&pageId, (void *)result);
		//发生淘汰，清理内存
		if(eliminateNode!=NULL){
			freeIndexTreeNode(eliminateNode, eliminateNode->type);
		}
		pageId = nodes[i]->after;
	}
	if(nodes[1]==NULL){
		result = nodes[0];
	} else {
		if(nodes[0]->nodeVersion>=engine->nextNodeVersion){
			result = nodes[1];
			freeIndexTreeNode(nodes[0], nodes[0]->type);
		} else if(nodes[1]->nodeVersion>=engine->nextNodeVersion){
			result = nodes[0];
			freeIndexTreeNode(nodes[1], nodes[1]->type);
		}
	}
	if(result!=NULL){
		result->pageId = pageIdBak;
		result->newPageId = pageIdBak;
		result->after = 0;
	} else {
		if (nodes[0]->nodeVersion < nodes[1]->nodeVersion){
			//有效数据在node[1]
			result = nodes[1];
			result->pageId = pageIdBak;
			result->newPageId = pageIdBak;
			result->after = nodes[0]->after;
			freeIndexTreeNode(nodes[0], nodes[0]->type);
		} else {
			//有效数据在node[0]
			result = nodes[0];
			result->pageId = pageIdBak;
			result->newPageId = result->after;
			result->after = nodes[0]->after;
			freeIndexTreeNode(nodes[1], nodes[1]->type);
		}
	}
	putLRUCache(unchangeCache, (uint8*)&result->pageId, result);
	return result;
}

// private IndexTreeNode* getEffectiveTreeNodeByPageId(IndexEngine *engine, uint64 pageId, int32 nodeType, uint64 parentPageId, int32 index){
// 	if(pageId==0){
// 		return NULL;
// 	}
// 	IndexTreeNode *node = getTreeNodeByPageId(engine, pageId, nodeType);
// 	if(node->effect==0){
// 		return node;
// 	}
// 	IndexTreeNode *effectNode = getTreeNodeByPageId(engine, node->effect, nodeType);
// 	//effectNode节点有效
// 	if(effectNode->nodeVersion<engine->nextNodeVersion){
// 		node = effectNode;
// 	}
// 	//当前节点是effect指向的节点，更新父节点
// 	if(node->pageId!=pageId){
		
// 	}
// 	return node;
// }

/*****************************************************************************
 * 公开API：文件操作
 ******************************************************************************/

IndexEngine *makeIndexEngine(
	char *filename,
	uint32 keyLen,
	uint32 valueLen,
	uint32 pageSize,
	int8 isUnique,
	uint64 maxHeapSize)
{
	//初始化为16k
	if(pageSize==0) pageSize = 16*1024; 
	if(maxHeapSize==0) maxHeapSize = 96*1024*1024;
	int wfd = createIndexFile(filename);
	int rfd = openIndexFile(filename);
	//异常情况判断：无法创建文件，或文件不存在
	if(wfd==-1 || rfd==-1) {
		return NULL;
	}
	//异常情况2：页大小过小
	if(pageSize<INDEX_META_SIZE){
		return NULL;
	}
	//异常情况3：度大于等于三的节点无法放到一个页中
	if((keyLen+8)*3+NODE_META_SIZE>pageSize){
		return NULL;
	}
	//异常情况4：一对kv无法放到一个页中
	if((keyLen+valueLen)*3+NODE_META_SIZE>pageSize){
		return NULL;
	}

	//创建并写入元数据
	IndexEngine *engine = (IndexEngine *)malloc(sizeof(IndexEngine));
	engine->filename = (char*)malloc(sizeof(char)*(strlen(filename)+1));
	strcpy(engine->filename,filename);
	engine->rfd = rfd;
	engine->wfd = wfd;
	engine->flag = 0;
	engine->nextNodeVersion = 2;
	SET_CREATING(engine->flag);
	if(isUnique){
		SET_UNIQUE(engine->flag);
	} else {
		CLR_UNIQUE(engine->flag);
	}
	engine->magic = MAGIC_NUMBER;
	engine->version = 1;
	engine->pageSize = pageSize;
	engine->nextPageId = 2;
	engine->usedPageCnt = 2;
	//注意树的度要保证一个节点（叶子或者非叶子）都能在一个页中存储
	engine->treeMeta.degree = (pageSize - NODE_META_SIZE) / (keyLen+(8>valueLen?8:valueLen));
	engine->treeMeta.keyLen = keyLen;
	engine->treeMeta.valueLen = valueLen;
	engine->treeMeta.depth = 1;
	engine->treeMeta.isUnique = isUnique;
	engine->treeMeta.root = 1;
	engine->treeMeta.sqt = 1;
	engine->count = 0;
	char metaBuffer[INDEX_META_SIZE];
	memset(metaBuffer, 0, INDEX_META_SIZE);
	metaToBuffer(engine, metaBuffer);
	writePageIndexFile(engine, 0, metaBuffer, INDEX_META_SIZE);
	//创建并写入根节点
	char rootBuffer[NODE_META_SIZE];
	memset(rootBuffer, 0, sizeof(rootBuffer));
	uint64 rootNodeVersion = 1ull;
	copyToBuffer(rootBuffer + 24, &rootNodeVersion, sizeof(rootNodeVersion));
	writePageIndexFile(engine, 1, rootBuffer, sizeof(rootBuffer));
	//创建缓存
	if(initIndexCache(engine, maxHeapSize)!=0){
		free(engine);
		return NULL;
	}
	//修改flag字段，强刷磁盘操作
	CLR_CREATING(engine->flag);
	writeTypePosition(engine, 12, &engine->flag, sizeof(engine->flag));
	fsync(engine->wfd);
	return engine;
}

IndexEngine *loadIndexEngine(char *filename, uint64 maxHeapSize)
{
	int rfd = openIndexFile(filename);
	int wfd = openIndexFile(filename);
	if (maxHeapSize == 0) maxHeapSize = 96 * 1024 * 1024;
	if(rfd==-1||wfd==-1) return NULL;
	IndexEngine* engine = (IndexEngine*) malloc(sizeof(IndexEngine));
	engine->rfd = rfd;
	engine->wfd = wfd;
	engine->filename = filename;
	char metaBuffer[INDEX_META_SIZE];
	engine->pageSize = INDEX_META_SIZE; //临时设置
	uint64 readLen = readPageIndexFile(engine, 0, metaBuffer, INDEX_META_SIZE);
	if(readLen<INDEX_META_SIZE) return NULL;
	bufferToMeta(engine, metaBuffer);
	uint32 flag = engine->flag;
	engine->treeMeta.isUnique = IS_UNIQUE(flag);
	//上次尚未创建完成，直接删除即可
	if(IS_CREATING(flag)){
		close(rfd);
		close(wfd);
		unlink(filename);
		free(engine);
		return NULL;
	}
	//在进行持久化的时候宕机
	if (IS_PERSISTENCE(flag)){
		//TODO 遍历树对废弃页进行标记
	}
	//在进行持久化-写元数据的时候宕机
	if (IS_SWITCHTREE(flag)){
		//TODO 进入恢复路径：从备份中恢复元数据
		//TODO 遍历树对废弃页进行标记
	}
	//TODO 进行碎片整理，根据配置是否进行
	//TODO 执行重做日志
	if(initIndexCache(engine, maxHeapSize)!=0){
		free(engine);
		return NULL;
	}
	return engine;
}


/*****************************************************************************
 * 私有函数：增删改查辅助函数
 ******************************************************************************/

/**
 * 针对B+树的一个节点的keys做二分查找
 * 找到小于等于key的第一个元素的下标，若不存在返回-1
 * 
 * 例如： root->keys = {5, 5, 7, 9}
 * key分别为     1  2 5 6 7 8 9 10
 * 则返回值分别为 -1 -1 0 1 2 2 3 3
 * 
 * @param root   B+树的一个节点
 * @param key    待查找的key字节数组的指针
 * @param keyLen 带查找的key字节数组的长度
 * @return root->keys中第一个小于等于key的元素下标，若不存在返回-1
 */
private int32 binarySearchNode(IndexTreeNode* root, uint8* key, uint32 keyLen, uint32 size){
	if(size==0){
		return -1;
	}
	int32 left = 0, right = size-1, mid;
	int32 result;
	while(left<right){
		mid = (left+right)>>1;
		result = byteArrayCompare(keyLen, root->keys[mid], key);
		if(result==0){ //keys[mid]==key
			right=mid;
		} else if(result<0){ //keys[mid]<key
			left=mid+1;
		} else { //keys[mid]>key
			right=mid-1;
		}
	}
	result = byteArrayCompare(keyLen, root->keys[left], key);
	if (result>0){
		return left-1;
	}
	return left;
}

/**
 * 在叶子节点中查找所有满足条件的value，放到List中
 * @param engine
 * @param key
 * @param leaf
 * @return {List} 一个包含数据的链表
 */
private List* getLeafNodeValues(IndexEngine *engine, uint8 *key, IndexTreeNode* leaf){
	int32 quickReturn = 0;
	List *result = makeList();
	IndexTreeMeta* treeMeta = &engine->treeMeta;
	int32 idx=-1;
	uint8 *value = NULL;
	do {
		idx = binarySearchNode(leaf, key, treeMeta->keyLen, leaf->size);
		if (idx == -1){
			return result;
		}
		//找到了第一个相等的元素
		if(0==byteArrayCompare(treeMeta->keyLen, leaf->keys[idx] , key)){
			if(!quickReturn){
				quickReturn = 1;
			}
			for (int i = idx; i < leaf->size; i++){
				if(i==idx || 0==byteArrayCompare(treeMeta->keyLen, leaf->keys[i] , key)){
					value = (uint8 *)malloc(treeMeta->valueLen);
					memcpy(value, leaf->values[i], treeMeta->valueLen);
					addList(result, (void*)value);
				} else {
					return result;
				}
			}
		} else {
			if(quickReturn || (idx!=leaf->size-1)){
				return result;
			}
		}
	} while ((leaf=getTreeNodeByPageId(engine, leaf->next, NODE_TYPE_LEAF))!=NULL);
	return result;
}

/**
 * 将现有节点分裂成两个节点，返回新创建的节点，平均分配
 */
static IndexTreeNode *splitTreeNode(IndexEngine *engine, IndexTreeNode* nowNode, int32 nodeType){
	IndexTreeNode *newNode = newIndexTreeNode(engine, nodeType);
	int len = nowNode->size / 2; //此时size == degree+1
	memcpy(newNode->keys, nowNode->keys+len, sizeof(uint8 *) * (nowNode->size - len));
	if(NODE_TYPE_LEAF==nodeType){
		memcpy(newNode->values, nowNode->values+len, sizeof(uint8*) * (nowNode->size-len));
	} else {
		memcpy(newNode->children, nowNode->children+len, sizeof(uint64) * (nowNode->size-len));
	}
	newNode->size = nowNode->size-len;
	nowNode->size = len;
	if(nodeType==NODE_TYPE_LEAF){
		//只有叶子节点才设置链表指针
		uint64 tmp = nowNode->next;
		nowNode->next = newNode->pageId;
		newNode->next = tmp;
		newNode->prev = nowNode->pageId;
		IndexTreeNode* nextNode = getTreeNodeByPageId(engine, tmp, nodeType);
		if(nextNode!=NULL){
			nextNode->prev = newNode->pageId;
		}
	}
	return newNode;
}

/** 递归进行插入及树重建 */
static IndexTreeNode* insertTo(IndexEngine *engine, uint64 pageId, uint8 *key, uint8 *value, int32 level){
	IndexTreeMeta *treeMeta = &engine->treeMeta;

	int32 index; 
	IndexTreeNode *now;
	//是叶子节点
	if (level == treeMeta->depth){
		now = getTreeNodeByPageId(engine, pageId, NODE_TYPE_LEAF);
		index = binarySearchNode(now, key, treeMeta->keyLen, now->size);
		insertToArray((void **)now->keys, treeMeta->degree + 1, index + 1, (void *)key);
		insertToArray((void **)now->values, treeMeta->degree + 1, index + 1, (void *)value);
		now->size++;
		changeIndexTreeNodeStatus(engine, now, NODE_STATUS_UPDATE);
		putTochangeCacheWork(engine, now);
		if (now->size <= treeMeta->degree){ //未满
			return NULL;
		}
		//已满
		IndexTreeNode *newNode = splitTreeNode(engine, now, NODE_TYPE_LEAF);
		putTochangeCacheWork(engine, newNode);
		
		return newNode;
	}
	now = getTreeNodeByPageId(engine, pageId, NODE_TYPE_LINK);
	index = binarySearchNode(now, key, treeMeta->keyLen, now->size);
	if(index==-1){
		now->keys[0] = key;
		index++;
	}
	uint64 next = now->children[index];
	IndexTreeNode *result = insertTo(engine, next, key, value, level + 1);
	//非叶子节点后续处理
	if(result==NULL){
		return NULL;
	}
	//防止now被淘汰
	now = getTreeNodeByPageId(engine, pageId, NODE_TYPE_LINK);
	uint8 *childKey = malloc(treeMeta->keyLen);
	memcpy(childKey, result->keys[0], treeMeta->keyLen);
	insertToArray((void **)now->keys, treeMeta->degree + 1, index + 1, (void *)childKey);
	insertToArray((void **)now->children, treeMeta->degree + 1, index + 1, (void *)result->pageId);
	now->size++;
	changeIndexTreeNodeStatus(engine, now, NODE_STATUS_UPDATE);
	putTochangeCacheWork(engine, now);
	if(now->size<=treeMeta->degree){ //未满
		return NULL;
	}
	//已满
	IndexTreeNode *newNode = splitTreeNode(engine, now, NODE_TYPE_LINK);
	putTochangeCacheWork(engine, newNode);
	return newNode;
}

/*****************************************************************************
 * 公开API：增删改查
 ******************************************************************************/
List *searchIndexEngine(IndexEngine *engine, uint8 *key){
	IndexTreeMeta* treeMeta = &engine->treeMeta;
	IndexTreeNode *node = NULL;
	uint64 pageId = treeMeta->root;
	int32 index = 0;
	//一直查找到叶子节点
	for(int32 level = 1; level<treeMeta->depth; level++){
		node = getTreeNodeByPageId(engine, pageId, NODE_TYPE_LINK);
		index = binarySearchNode(node, key, treeMeta->keyLen, node->size);
		if(index<0){
			return makeList();
		}
		pageId = node->children[index];
	}
	node = getTreeNodeByPageId(engine, pageId, NODE_TYPE_LEAF);
	return getLeafNodeValues(engine, key, node);
}

int32 insertIndexEngine(IndexEngine *engine, uint8 *key, uint8 *value){
	IndexTreeMeta* treeMeta = &engine->treeMeta;
	//违反唯一约束
	if (treeMeta->isUnique){
		List* list =searchIndexEngine(engine, key);
		if(list->length!=0){
			return -1;
		}
	}
	uint8 *newKey = NULL, *newValue=NULL;
	newAndCopyByteArray(&newKey, key, engine->treeMeta.keyLen);
	newAndCopyByteArray(&newValue, value, engine->treeMeta.valueLen);
	IndexTreeNode *newChild = insertTo(engine, engine->treeMeta.root, newKey, newValue, 1);
	if (newChild != NULL)
	{
		IndexTreeNode *newRoot = newIndexTreeNode(engine, NODE_TYPE_LINK);
		IndexTreeNode *oldRoot = getTreeNodeByPageId(
			engine,
			treeMeta->root,
			treeMeta->depth == 1 ? NODE_TYPE_LEAF : NODE_TYPE_LINK);
		newAndCopyByteArray(&newRoot->keys[0], oldRoot->keys[0], treeMeta->keyLen);
		newAndCopyByteArray(&newRoot->keys[1], newChild->keys[0], treeMeta->keyLen);
		newRoot->children[0] = oldRoot->pageId;
		newRoot->children[1] = newChild->pageId;
		newRoot->size = 2;
		treeMeta->root = newRoot->pageId;
		treeMeta->depth++;
		putTochangeCacheWork(engine,newRoot);
	}
	engine->count++; //计数
	checkThreadPersistence(engine);
	return 1;
}

/*****************************************************************************
 * 辅助函数
 ******************************************************************************/

#ifndef PROFILE_TEST
void flushIndexEngine(IndexEngine **engines){
	IndexEngine *engine = engines[0];
	IndexEngine *freezeEngine = engines[1];
	uint32 diskFlag = engine->flag;

	//开始持久化
	//切换到正在持久化状态
	SET_PERSISTENCE(diskFlag);
	writeTypePosition(engine, 12, &diskFlag, sizeof(diskFlag));
	fsync(engine->wfd);
	LRUCache *freezeCache = engine->cache.changeCacheFreeze;
	
	LRUNode* node = freezeCache->head;
	char *buffer = (char *)malloc(engine->pageSize);
	while((node=node->next)!=freezeCache->head){
		IndexTreeNode *treeNode = (IndexTreeNode *)node->value;
		nodeToBuffer(engine, treeNode, treeNode->type, buffer);
		uint32 len = NODE_META_SIZE + treeNode->size * (
			engine->treeMeta.keyLen +
			(treeNode->type == NODE_TYPE_LINK ? 8 : engine->treeMeta.valueLen)
		);
		writePageIndexFile(engine, treeNode->newPageId, buffer, len);
		//更新类型页，设置after字段
		if(treeNode->status==NODE_STATUS_UPDATE && treeNode->pageId!=treeNode->newPageId){
			writeTypePosition(
				engine,
				treeNode->pageId * engine->pageSize + 16,
				&treeNode->after,
				sizeof(treeNode->after));
		}
	}
	//备份磁盘中重要元数据
	writeMetaBackData(engine);
	//磁盘状态：切换到切换树状态
	CLR_PERSISTENCE(diskFlag);
	SET_SWITCHTREE(diskFlag);
	writeTypePosition(engine, 12, &diskFlag, sizeof(diskFlag));
	//确保数据写入
	fsync(engine->wfd);
	//完成树切换：将新的元数据可入磁盘
	char* metaBuffer = (char*) malloc(INDEX_META_SIZE_NO_BACK);
	metaToBuffer(freezeEngine, metaBuffer);
	writePageIndexFile(engine, 0, metaBuffer, INDEX_META_SIZE_NO_BACK);
	fsync(engine->wfd);
	//磁盘状态：切换到正常状态
	CLR_SWITCHTREE(diskFlag);
	writeTypePosition(engine, 12, &diskFlag, sizeof(diskFlag));
	fsync(engine->wfd);
	//线程状态：恢复到NORMAL状态
	pthread_cleanup_push((void *)pthread_mutex_unlock, engine->cache.statusMutex);
	pthread_mutex_lock(engine->cache.statusMutex);
	engine->cache.status = CACHE_STATUS_NORMAL;
	//清空缓存缓存
	clearLRUCache(freezeCache);
	//通知其他阻塞线程
	pthread_cond_signal(engine->cache.statusCond);
	pthread_mutex_unlock(engine->cache.statusMutex);
	pthread_cleanup_pop(0);
	//清空内存
	free(freezeEngine);
	free(engines);
}
#else
int persistenceExceptionId=0;
void flushIndexEngine(IndexEngine **engines){
	IndexEngine *engine = engines[0];
	IndexEngine *freezeEngine = engines[1];
	uint32 diskFlag = engine->flag;

	//开始持久化
	//切换到正在持久化状态
	SET_PERSISTENCE(diskFlag);
	if(persistenceExceptionId==1) return;
	writeTypePosition(engine, 12, &diskFlag, sizeof(diskFlag));
	if(persistenceExceptionId==2) return;
	fsync(engine->wfd);
	if(persistenceExceptionId==3) return;
	LRUCache *freezeCache = engine->cache.changeCacheFreeze;
	
	LRUNode* node = freezeCache->head;
	char *buffer = (char *)malloc(engine->pageSize);
	while((node=node->next)!=freezeCache->head){
		IndexTreeNode *treeNode = (IndexTreeNode *)node->value;
		nodeToBuffer(engine, treeNode, treeNode->type, buffer);
		uint32 len = NODE_META_SIZE + treeNode->size * (
			engine->treeMeta.keyLen +
			(treeNode->type == NODE_TYPE_LINK ? 8 : engine->treeMeta.valueLen)
		);
		writePageIndexFile(engine, treeNode->newPageId, buffer, len);
		//更新类型页，设置after字段
		if(treeNode->status==NODE_STATUS_UPDATE && treeNode->pageId!=treeNode->newPageId){
			writeTypePosition(
				engine,
				treeNode->pageId * engine->pageSize + 16,
				&treeNode->after,
				sizeof(treeNode->after));
		}
	}
	if(persistenceExceptionId==4) return;
	//备份磁盘中重要元数据
	writeMetaBackData(engine);
	if(persistenceExceptionId==5) return;
	//磁盘状态：切换到切换树状态
	CLR_PERSISTENCE(diskFlag);
	SET_SWITCHTREE(diskFlag);
	writeTypePosition(engine, 12, &diskFlag, sizeof(diskFlag));
	if(persistenceExceptionId==6) return;
	//确保数据写入
	fsync(engine->wfd);
	if(persistenceExceptionId==7) return;
	//完成树切换：将新的元数据可入磁盘
	char* metaBuffer = (char*) malloc(INDEX_META_SIZE_NO_BACK);
	metaToBuffer(freezeEngine, metaBuffer);
	writePageIndexFile(engine, 0, metaBuffer, INDEX_META_SIZE_NO_BACK);
	if(persistenceExceptionId==8) return;
	fsync(engine->wfd);
	if(persistenceExceptionId==9) return;
	//磁盘状态：切换到正常状态
	CLR_SWITCHTREE(diskFlag);
	writeTypePosition(engine, 12, &diskFlag, sizeof(diskFlag));
	if(persistenceExceptionId==10) return;
	fsync(engine->wfd);
	if(persistenceExceptionId==11) return;
	//线程状态：恢复到NORMAL状态
	pthread_cleanup_push((void *)pthread_mutex_unlock, engine->cache.statusMutex);
	pthread_mutex_lock(engine->cache.statusMutex);
	engine->cache.status = CACHE_STATUS_NORMAL;
	if(persistenceExceptionId==12) return;
	//清空缓存缓存
	clearLRUCache(freezeCache);
	//通知其他阻塞线程
	pthread_cond_signal(engine->cache.statusCond);
	pthread_mutex_unlock(engine->cache.statusMutex);
	pthread_cleanup_pop(0);
	//清空内存
	free(freezeEngine);
	free(engines);
}
#endif