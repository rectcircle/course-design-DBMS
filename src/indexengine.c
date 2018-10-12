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
//元数据长度
static const uint32 MAX_META_SIZE=120;

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
}

private void nodeToBuffer(IndexEngine* engine, IndexTreeNode *node, int32 nodeType, char* buffer){
	int len = 0;
	if(nodeType==NODE_TYPE_LEAF_NO_META){
		len += copyToBuffer(buffer + len, &node->after, sizeof(node->after));
		len += copyToBuffer(buffer + len, &node->flag, sizeof(node->flag));
	} else {
		len += copyToBuffer(buffer + len, &node->next, sizeof(node->next));
		len += copyToBuffer(buffer + len, &node->effect, sizeof(node->effect));
		len += copyToBuffer(buffer + len, &node->size, sizeof(node->size));
		len += copyToBuffer(buffer + len, &node->flag, sizeof(node->flag));
		len += copyToBuffer(buffer + len, &node->after, sizeof(node->after));
	}
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
	uint16 *dest2 = (uint16 *) data;
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
}

private void bufferToNode(
	IndexEngine *engine,
	IndexTreeNode *node,
	int32 nodeType,
	char *buffer){

	node->status = NODE_STATUS_OLD;
	node->type = nodeType;
	
	int len = 0;
	if(nodeType==NODE_TYPE_LEAF_NO_META){
		len += parseFromBuffer(buffer + len, &node->after, sizeof(node->after));
		len += parseFromBuffer(buffer + len, &node->flag, sizeof(node->flag));
	} else {
		len += parseFromBuffer(buffer + len, &node->next, sizeof(node->next));
		len += parseFromBuffer(buffer + len, &node->effect, sizeof(node->effect));
		len += parseFromBuffer(buffer + len, &node->size, sizeof(node->size));
		len += parseFromBuffer(buffer + len, &node->flag, sizeof(node->flag));
		len += parseFromBuffer(buffer + len, &node->after, sizeof(node->after));
	}
	uint32 keyLen = engine->treeMeta.keyLen;
	uint32 valueLen = engine->treeMeta.valueLen;
	node->keys = (uint8**) malloc(sizeof(uint8*)*(engine->treeMeta.degree+1));
	if(nodeType==NODE_TYPE_LINK){
		node->children = (uint64*)malloc(sizeof(uint64)*(engine->treeMeta.degree+1));
		for (int i = 0; i < node->size; i++){
			node->keys[i] = (uint8*)malloc(sizeof(uint8)*engine->treeMeta.keyLen);
			memcpy(node->keys[i], buffer + len, keyLen);
			len += keyLen;
			len += parseFromBuffer(buffer + len, &node->children[i], sizeof(node->children[i]));
		}
	} else {
		node->values = (uint8**) malloc(sizeof(uint8*)*(engine->treeMeta.degree+1));
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
	lseek(engine->fd, pageId*engine->pageSize, SEEK_SET);
	return write(engine->fd, buffer, len);
}

private uint64 readPageIndexFile(IndexEngine* engine, uint64 pageId, char *buffer, uint32 len){
	lseek(engine->fd, pageId * engine->pageSize, SEEK_SET);
	return read(engine->fd, buffer, len);
}

/** 根据最大堆内存大小，创建引擎需要用到的缓存,返回0表示成功 */
static int32 initIndexCache(IndexEngine* engine, uint64 maxHeapSize){
	uint32 capacity = (maxHeapSize/3 - 32) / (32/3+48+engine->treeMeta.keyLen+engine->treeMeta.valueLen);
	if(capacity<3){
		return -1;
	}
	engine->cache.unchangeCache = makeLRUCache(capacity, sizeof(engine->treeMeta.root));
	engine->cache.changeCacheWork = makeLRUCache(capacity, sizeof(engine->treeMeta.root));
	engine->cache.changeCacheFreeze = makeLRUCache(capacity, sizeof(engine->treeMeta.root));
	engine->cache.status = 0;
	pthread_cond_init(&engine->cache.statusCond, NULL);
	pthread_mutex_init(&engine->cache.statusMutex, NULL);
	return 0;
}

/** 切换change缓存 */
static void swapChangeCache(IndexEngine *engine){
	LRUCache* tmp = engine->cache.changeCacheFreeze;
	engine->cache.changeCacheFreeze = engine->cache.changeCacheWork;
	engine->cache.changeCacheWork = tmp;
}

static void cacheEliminateHook(uint32 keyLen, uint8 *key, void *value) {
	free(key);
}

private IndexTreeNode* getTreeNodeByPageId(IndexEngine *engine, uint64 pageId, int32 nodeType){
	LRUCache *changeCacheWork = engine->cache.changeCacheWork;
	LRUCache *changeCacheFreeze = engine->cache.changeCacheFreeze;
	LRUCache *unchangeCache = engine->cache.unchangeCache;
	uint64 *key = NULL;

	//首先从changeCacheWork中获取
	IndexTreeNode *result = getLRUCache(changeCacheWork, &pageId);
	if(result!=NULL){
		//可以获取到直接返回
		return result;
	}

	//否则判断是否需要在changeCacheFreeze中获取
	pthread_mutex_lock(&engine->cache.statusMutex);
	//当缓存处于切换工作、冻结缓存的时候，等待
	while(engine->cache.status==CACHE_STATUS_SWITCH_CHANGE){
		pthread_cond_wait(&engine->cache.statusCond, &engine->cache.statusMutex);
	}
	//当前处于持久化中的状态，搜索冻结的缓存
	if(engine->cache.status==CACHE_STATUS_PERSISTENCE){
		result = getLRUCache(changeCacheFreeze, &pageId);
	}
	//获取到了，插入到changeCacheWork
	if (result != NULL){
		key = (uint64*) malloc(sizeof(uint64));
		*key = pageId;
		putLRUCache(changeCacheWork, key, (void*)result);
	}
	//changeCacheWork满了
	if(changeCacheWork->size==changeCacheWork->capacity){
		//等待持久化线程结束
		while (engine->cache.status != CACHE_STATUS_NORMAL){
			pthread_cond_wait(&engine->cache.statusCond, &engine->cache.statusMutex);
		}
		//进行cache切换
		engine->cache.status = CACHE_STATUS_SWITCH_CHANGE;
		swapChangeCache(engine);
		//TODO 启动持久化线程（将状态设为持久化）
	}
	pthread_mutex_unlock(&engine->cache.statusMutex);
	if (result != NULL){
		return result;
	}

	//从unchangeCache中读取
	result = getLRUCache(unchangeCache, &pageId);
	if(result!=NULL){
		return result;
	}

	//从文件中读取
	result = (IndexTreeNode*)malloc(sizeof(IndexTreeNode));
	memset((void*)result, 0, sizeof(IndexTreeNode));
	char * buffer = (char*)malloc(sizeof(char)*engine->pageSize);
	readPageIndexFile(engine, pageId, buffer, engine->pageSize);
	bufferToNode(engine, result, nodeType, buffer);
	key = (uint64 *)malloc(sizeof(uint64));
	*key = pageId;
	IndexTreeNode *eliminateNode = (IndexTreeNode *)putLRUCacheWithHook(unchangeCache, key, (void *)result, cacheEliminateHook);
	//没有发生淘汰
	if(eliminateNode==NULL){
		return result;
	}
	//发生淘汰，且淘汰的节点不是和磁盘中一致，放到，changeCacheWork中
	if(eliminateNode->status!=NODE_STATUS_OLD){
		key = (uint64 *)malloc(sizeof(uint64));
		*key = pageId;
		putLRUCache(changeCacheWork, key, eliminateNode);
	}
		//changeCacheWork满了
	if(changeCacheWork->size==changeCacheWork->capacity){
		pthread_mutex_lock(&engine->cache.statusMutex);
		while (engine->cache.status != CACHE_STATUS_NORMAL){
			pthread_cond_wait(&engine->cache.statusCond, &engine->cache.statusMutex);
		}
		//进行cache切换
		engine->cache.status = CACHE_STATUS_SWITCH_CHANGE;
		swapChangeCache(engine);
		//TODO 启动持久化线程（将状态设为持久化）
		pthread_mutex_unlock(&engine->cache.statusMutex);
	}
	return result;
}

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
	int fd = createIndexFile(filename);
	//异常情况判断：无法创建文件，或文件不存在
	if(fd==-1) {
		return NULL;
	}
	//异常情况2：页大小过小
	if(pageSize<MAX_META_SIZE){
		return NULL;
	}
	//异常情况3：度大于等于三的节点无法放到一个页中
	if((keyLen+8)*3+32>pageSize){
		return NULL;
	}
	//异常情况4：一对kv无法放到一个页中
	if((keyLen+valueLen)+12>pageSize){
		return NULL;
	}

	//创建并写入元数据
	IndexEngine *engine = (IndexEngine *)malloc(sizeof(IndexEngine));
	engine->filename = filename;
	engine->fd = fd;
	engine->flag = 0;
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
	engine->treeMeta.degree = (pageSize - 32) / (keyLen+8);
	engine->treeMeta.keyLen = keyLen;
	engine->treeMeta.valueLen = valueLen;
	engine->treeMeta.depth = 1;
	engine->treeMeta.isUnique = isUnique;
	engine->treeMeta.root = 1;
	engine->treeMeta.sqt = 1;
	engine->count = 0;
	char metaBuffer[MAX_META_SIZE];
	memset(metaBuffer, 0, MAX_META_SIZE);
	metaToBuffer(engine, metaBuffer);
	writePageIndexFile(engine, 0, metaBuffer, MAX_META_SIZE);
	//创建并写入根节点
	//根节点所有元数据为0即可
	char rootBuffer[32];
	memset(rootBuffer, 0, sizeof(rootBuffer));
	writePageIndexFile(engine, 1, rootBuffer, sizeof(rootBuffer));
	//创建缓存
	if(initIndexCache(engine, maxHeapSize)!=0){
		free(engine);
		return NULL;
	}
	//修改flag字段，强刷磁盘操作
	CLR_CREATING(engine->flag);
	uint32 flag = htonl(engine->flag);
	lseek(engine->fd, 12, SEEK_SET);
	write(engine->fd, &flag, sizeof(flag));
	fsync(engine->fd);
	return engine;
}

IndexEngine *loadIndexEngine(char *filename, uint64 maxHeapSize)
{
	int fd = openIndexFile(filename);
	if(fd==-1) return NULL;
	IndexEngine* engine = (IndexEngine*) malloc(sizeof(IndexEngine));
	engine->fd = fd;
	engine->filename = filename;
	char metaBuffer[MAX_META_SIZE];
	engine->pageSize = MAX_META_SIZE; //临时设置
	uint64 readLen = readPageIndexFile(engine, 0, metaBuffer, MAX_META_SIZE);
	if(readLen<MAX_META_SIZE) return NULL;
	bufferToMeta(engine, metaBuffer);
	uint32 flag = engine->flag;
	engine->treeMeta.isUnique = IS_UNIQUE(flag);
	//上次尚未创建完成，直接删除即可
	if(IS_CREATING(flag)){
		close(fd);
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
 * 公开API：查询
 ******************************************************************************/
Array *searchIndexEngine(IndexEngine *engine, uint8 *key){
	int32 rootType;
	if(engine->treeMeta.depth==1){
		rootType = NODE_TYPE_LEAF_WITH_META;
	} else {
		rootType = NODE_TYPE_LINK;
	}
	IndexTreeNode *root = getTreeNodeByPageId(engine, engine->treeMeta.root, rootType);
	//TODO 完成
}
