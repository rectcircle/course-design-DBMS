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
static const uint32 INDEX_META_SIZE=136;
//节点元数据长度
static const uint32 NODE_META_SIZE = 40;

/*****************************************************************************
 * 私有函数：申请释结构放内存
 ******************************************************************************/

/** 创建一个Node */
private IndexTreeNode* makeIndexTreeNode(IndexEngine* engine, int32 nodeType){
	IndexTreeNode *node = (IndexTreeNode *)malloc(sizeof(IndexTreeNode));
	node->keys = (uint8 **)malloc(sizeof(uint8 *) * (engine->treeMeta.degree + 1));
	if (nodeType == NODE_TYPE_LINK){
		node->children = (uint64 *)malloc(sizeof(uint64) * (engine->treeMeta.degree + 1));
	} else {
		node->values = (uint8 **)malloc(sizeof(uint8 *) * (engine->treeMeta.degree + 1));
	}
	return node;
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
	dest->effect = src->effect;
	dest->after = src->after;
	dest->nodeVersion = src->nodeVersion;
	dest->status = src->status;

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
	if(nodeType==NODE_TYPE_LEAF_NO_META){
		len += copyToBuffer(buffer + len, &node->after, sizeof(node->after));
		len += copyToBuffer(buffer + len, &node->flag, sizeof(node->flag));
		len += copyToBuffer(buffer + len, &node->kvLength, sizeof(node->kvLength));
		len += copyToBuffer(buffer + len, &node->nodeVersion, sizeof(node->nodeVersion));
		len += copyToBuffer(buffer + len, &node->effect, sizeof(node->effect));
	} else {
		len += copyToBuffer(buffer + len, &node->next, sizeof(node->next));
		len += copyToBuffer(buffer + len, &node->effect, sizeof(node->effect));
		len += copyToBuffer(buffer + len, &node->size, sizeof(node->size));
		len += copyToBuffer(buffer + len, &node->flag, sizeof(node->flag));
		len += copyToBuffer(buffer + len, &node->after, sizeof(node->after));
		len += copyToBuffer(buffer + len, &node->nodeVersion, sizeof(node->nodeVersion));
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
	if(nodeType==NODE_TYPE_LEAF_NO_META){
		len += parseFromBuffer(buffer + len, &node->after, sizeof(node->after));
		len += parseFromBuffer(buffer + len, &node->flag, sizeof(node->flag));
		len += parseFromBuffer(buffer + len, &node->kvLength, sizeof(node->kvLength));
		len += parseFromBuffer(buffer + len, &node->nodeVersion, sizeof(node->nodeVersion));
		len += parseFromBuffer(buffer + len, &node->effect, sizeof(node->effect));
	} else {
		len += parseFromBuffer(buffer + len, &node->next, sizeof(node->next));
		len += parseFromBuffer(buffer + len, &node->effect, sizeof(node->effect));
		len += parseFromBuffer(buffer + len, &node->size, sizeof(node->size));
		len += parseFromBuffer(buffer + len, &node->flag, sizeof(node->flag));
		len += parseFromBuffer(buffer + len, &node->after, sizeof(node->after));
		len += parseFromBuffer(buffer + len, &node->nodeVersion, sizeof(node->nodeVersion));
	}
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
	lseek(engine->fd, pageId*engine->pageSize, SEEK_SET);
	return write(engine->fd, buffer, len);
}

private uint64 readPageIndexFile(IndexEngine* engine, uint64 pageId, char *buffer, uint32 len){
	lseek(engine->fd, pageId * engine->pageSize, SEEK_SET);
	return read(engine->fd, buffer, len);
}

/** 根据最大堆内存大小，创建引擎需要用到的缓存,返回0表示成功 */
static int32 initIndexCache(IndexEngine* engine, uint64 maxHeapSize){
	uint32 capacity = (maxHeapSize/3 - 32) / (32/3+56+engine->treeMeta.keyLen+engine->treeMeta.valueLen);
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
	IndexTreeNode *result = getLRUCache(changeCacheWork, (uint8 *)&pageId);
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
		result = getLRUCache(changeCacheFreeze, (uint8*)&pageId);
	}
	//获取到了，将其拷贝插入到changeCacheWork
	if (result != NULL){
		key = (uint64*) malloc(sizeof(uint64));
		IndexTreeNode* copyNode = copyIndexTreeNode(engine, result);
		copyNode->status = NODE_STATUS_OLD;
		*key = pageId;
		putLRUCache(changeCacheWork, (uint8*)key, (void*)copyNode);
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
	result = getLRUCache(unchangeCache, (uint8*)&pageId);
	if(result!=NULL){
		return result;
	}

	//从文件中读取
	result = makeIndexTreeNode(engine, nodeType);
	memset((void*)result, 0, sizeof(IndexTreeNode));
	char * buffer = (char*)malloc(sizeof(char)*engine->pageSize);
	readPageIndexFile(engine, pageId, buffer, engine->pageSize);
	bufferToNode(engine, result, nodeType, buffer);
	key = (uint64 *)malloc(sizeof(uint64));
	*key = pageId;
	IndexTreeNode *eliminateNode = (IndexTreeNode *)putLRUCacheWithHook(unchangeCache, (uint8*)key, (void *)result, cacheEliminateHook);
	//没有发生淘汰
	if(eliminateNode==NULL){
		return result;
	}
	//发生淘汰，且淘汰的节点不是和磁盘中一致，放到，changeCacheWork中
	if(eliminateNode->status!=NODE_STATUS_OLD){
		key = (uint64 *)malloc(sizeof(uint64));
		*key = pageId;
		putLRUCache(changeCacheWork, (uint8*)key, eliminateNode);
	} else {
		//直接free掉内存
		freeIndexTreeNode(eliminateNode, eliminateNode->type);
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


/** 一个赋值函数，将effect数据复制到指定位置 */
static void changeFromEffectNode(IndexEngine* engine, IndexTreeNode *dest, IndexTreeNode* src){
	// dest->pageId = src->pageId;
	// dest->newPageId = src->newPageId;
	dest->type = src->type;
	dest->size = src->size;
	dest->flag = src->flag;
	dest->next = src->next;
	dest->effect = 0;
	dest->after = src->after;
	dest->nodeVersion = src->nodeVersion;
	dest->status = NODE_STATUS_UPDATE;

	//拷贝数据
	if(src->type==NODE_TYPE_LINK){
		for (int i = 0; i < src->size; i++){
			free(dest->keys[i]);
			dest->keys[i] = (uint8*)malloc(sizeof(uint8)*engine->treeMeta.keyLen);
			memcpy(dest->keys[i], src->keys[i], engine->treeMeta.keyLen);
			dest->children[i] = src->children[i];
		}
	} else {
		for (int i = 0; i < src->size; i++){
			free(dest->keys[i]);
			dest->keys[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.keyLen);
			memcpy(dest->keys[i], src->keys[i], engine->treeMeta.keyLen);
			free(dest->values[i]);
			dest->values[i] = (uint8 *)malloc(sizeof(uint8) * engine->treeMeta.valueLen);
			memcpy(dest->values[i], src->values[i], engine->treeMeta.valueLen);
		}
	}
}

private IndexTreeNode* getEffectiveTreeNodeByPageId(IndexEngine *engine, uint64 pageId, int32 nodeType){
	if(pageId==0){
		return NULL;
	}
	IndexTreeNode *node = getTreeNodeByPageId(engine, pageId, nodeType);
	if(node->effect==0){
		return node;
	}
	IndexTreeNode *effectNode = getTreeNodeByPageId(engine, node->effect, nodeType);
	//该节点为废弃节点
	if(effectNode->nodeVersion>=engine->nextNodeVersion){
		return node;
	}
	//将node节点更新为effect节点的值，状态切换为update，effect设为0
	changeFromEffectNode(engine, node, effectNode);
	return node;
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
	if(pageSize<INDEX_META_SIZE){
		return NULL;
	}
	//异常情况3：度大于等于三的节点无法放到一个页中
	if((keyLen+8)*3+NODE_META_SIZE>pageSize){
		return NULL;
	}
	//异常情况4：一对kv无法放到一个页中
	if((keyLen+valueLen)+NODE_META_SIZE>pageSize){
		return NULL;
	}

	//创建并写入元数据
	IndexEngine *engine = (IndexEngine *)malloc(sizeof(IndexEngine));
	engine->filename = filename;
	engine->fd = fd;
	engine->flag = 0;
	engine->leafMaxKVLen = (pageSize-NODE_META_SIZE) / (keyLen+valueLen);
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
	engine->treeMeta.degree = (pageSize - 32) / (keyLen+8);
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
	//设置写入版本
	uint64 rootNodeVersion = 1ull;
	copyToBuffer(rootBuffer + 32, (void*)&rootNodeVersion, 8);
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
	char metaBuffer[INDEX_META_SIZE];
	engine->pageSize = INDEX_META_SIZE; //临时设置
	uint64 readLen = readPageIndexFile(engine, 0, metaBuffer, INDEX_META_SIZE);
	if(readLen<INDEX_META_SIZE) return NULL;
	bufferToMeta(engine, metaBuffer);
	uint32 flag = engine->flag;
	engine->leafMaxKVLen = (engine->pageSize - NODE_META_SIZE) / (engine->treeMeta.keyLen + engine->treeMeta.valueLen);
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
private List* searchLeafNode(IndexEngine *engine, uint8 *key, IndexTreeNode* leaf){
	int32 quickReturn = 0;
	List *result = makeList();
	IndexTreeMeta* treeMeta = &engine->treeMeta;
	int32 idx=-1;
	uint8 *value = NULL;
	do {
		uint32 nowSize = leaf->size;
		if(leaf->size>engine->leafMaxKVLen){
			nowSize = engine->leafMaxKVLen;
		}
		idx = binarySearchNode(leaf, key, treeMeta->keyLen, nowSize);
		if (idx == -1){
			return result;
		}
		//找到了第一个相等的元素
		if(0==byteArrayCompare(treeMeta->keyLen, leaf->keys[idx] , key)){
			if(!quickReturn){
				quickReturn = 1;
			}
			for (int i = idx; i < nowSize; i++){
				if(i==idx || 0==byteArrayCompare(treeMeta->keyLen, leaf->keys[idx] , key)){
					value = (uint8 *)malloc(treeMeta->valueLen);
					memcpy(value, leaf->values[i], treeMeta->valueLen);
					addList(result, (void*)value);
				} else {
					return result;
				}
			}
		} else {
			if(quickReturn || idx!=nowSize-1){
				return result;
			}
		}

		IndexTreeNode *leafOther = leaf;
		while((leafOther = getEffectiveTreeNodeByPageId(engine, leaf->after, NODE_TYPE_LEAF_NO_META))!=NULL){
			idx = binarySearchNode(leafOther, key, treeMeta->keyLen, leafOther->kvLength);
			if (idx == -1){
				return result;
			}
			//找到了第一个相等的元素
			if(0==byteArrayCompare(treeMeta->keyLen, leafOther->keys[idx] , key)){
				if(!quickReturn){
					quickReturn = 1;
				}
				for (int i = idx; i < leafOther->kvLength; i++){
					if(i==idx || 0==byteArrayCompare(treeMeta->keyLen, leafOther->keys[idx] , key)){
						value = (uint8 *)malloc(treeMeta->valueLen);
						memcpy(value, leafOther->values[i], treeMeta->valueLen);
						addList(result, (void*)value);
					} else {
						return result;
					}
				}
			} else {
				if(quickReturn || idx!=leafOther->kvLength-1){
					return result;
				}
			}	
		}
	} while ((leaf=getEffectiveTreeNodeByPageId(engine, leaf->next, NODE_TYPE_LEAF_WITH_META))!=NULL);
	return result;
}

/*****************************************************************************
 * 公开API：增删改查
 ******************************************************************************/
List *searchIndexEngine(IndexEngine *engine, uint8 *key){
	IndexTreeMeta* treeMeta = &engine->treeMeta;
	IndexTreeNode *node = NULL;
	uint64 pageId = treeMeta->root;
	//一直查找到叶子节点
	for(int32 level = 1; level<treeMeta->depth; level++){
		node = getEffectiveTreeNodeByPageId(engine, pageId, NODE_TYPE_LINK);
		int32 index = binarySearchNode(node, key, treeMeta->keyLen, node->size);
		pageId = node->children[index];
		if(index<0){
			return makeList();
		}
	}
	node = getEffectiveTreeNodeByPageId(engine, pageId, NODE_TYPE_LEAF_WITH_META);
	return searchLeafNode(engine, key, node);
}
