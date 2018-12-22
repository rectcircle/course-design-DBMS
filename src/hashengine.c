/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: indexengine.cpp
 * @description: Hash存储引擎API
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-12-08
 ******************************************************************************/

#include "hashengine.h"
#include "hashmap.h"
#include "lrucache.h"
#include "util.h"
#include "global.h"

#include <malloc.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

//魔数
static const uint32 MAGIC_NUMBER = 0x960729abu;

/*****************************************************************************
 * 私有函数：文件操作、序列化、反序列化、线程启动函数
 ******************************************************************************/

static int createHashFile(const char *filename)
{
	return open(filename, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

static int openHashFile(const char *filename)
{
	return open(filename, O_RDWR);
}

static void writeMetadata(int wfd){
	uint32 magic = htonl(MAGIC_NUMBER);
	uint32 version = htonl(1);
	lseek(wfd, 0, SEEK_SET);
	write(wfd, (void *)&magic, 4);
	write(wfd, (void *)&version, 4);
	fsync(wfd);
}

static int verifyMetadata(int rfd){
	uint32 magic ,version;
	lseek(rfd, 0, SEEK_SET);
	read(rfd, &magic, 4);
	read(rfd, &version, 4);
	return ntohl(magic)==MAGIC_NUMBER && ntohl(version) == 1;
}

static uint64 writeRecord(int wfd, Record* record){
	uint64 position = lseek(wfd, 0, SEEK_CUR);
	uint64 version = htonll(record->version);
	uint32 keyLen = htonl(record->keyLen);
	uint32 valueLen = htonl(record->valueLen);
	write(wfd, (void*)&version, 8);
	write(wfd, (void *)&keyLen, 4);
	write(wfd, (void *)&valueLen, 4);
	write(wfd, record->key, record->keyLen);
	write(wfd, record->value,record->valueLen);
	fsync(wfd);
	return position;
}

static Record *loadRecord(int rfd, uint64 position, int skipValue){
	Record *record = (Record *)malloc(sizeof(Record));
	lseek(rfd, position, SEEK_SET);
	read(rfd, &record->version, 8);
	read(rfd, &record->keyLen, 4);
	read(rfd, &record->valueLen, 4);
	record->version = ntohll(record->version);
	record->keyLen = ntohl(record->keyLen);
	record->valueLen = ntohl(record->valueLen);
	record->key = (uint8*) malloc(record->keyLen);
	read(rfd, record->key, record->keyLen);
	if (skipValue){
		record->value = NULL;
	} else {
		record->value = (uint8 *)malloc(record->valueLen);
		read(rfd, record->value, record->valueLen);
	}
	return record;
}

static void startPersistenceThread(HashEngine* engine){
	//启动持久化线程
	pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
	pthread_mutex_lock(&engine->statusMutex);
	while(engine->persistenceStatus!=None){
		pthread_cond_wait(&engine->statusCond, &engine->statusMutex);
	}
	LRUCache * tmp = engine->writeCache;
	engine->writeCache = engine->freezeWriteCache;
	engine->freezeWriteCache = tmp;
	engine->persistenceStatus = Doing;
	pthread_mutex_unlock(&engine->statusMutex);
	pthread_cleanup_pop(0);
	pthread_create(&engine->persistenceThread, NULL, (void *)flushHashEngine, (void *)engine);
}

/*****************************************************************************
 *构造、析构函数
 ******************************************************************************/

static RecordLocation* makeRecordLocation(uint64 id, uint64 position){
	RecordLocation *location = (RecordLocation *)malloc(sizeof(RecordLocation));
	location->id = id;
	location->position = position;
	return location;
}

static void freeRecordLocation(RecordLocation* location){
	free(location);
}

static void* freeHashMapRecordLocation(struct Entry * entry, void* args){
	freeRecordLocation((RecordLocation*) entry->value);
	return NULL;
}

static Record *makeRecord(uint64 version, uint32 keyLen, uint32 valueLen, uint8 *key, uint8 *value){
	Record *record = (Record*) malloc(sizeof(Record));
	record->version = version;
	record->keyLen = keyLen;
	record->valueLen = valueLen;
	uint8* copyKey, *copyValue;
	newAndCopyByteArray(&copyKey, key, keyLen);
	newAndCopyByteArray(&copyValue, value, valueLen);
	record->key = copyKey;
	record->value = copyValue;
	return record;
}

static void freeRecord(Record *record){
	if(record->key!=NULL){
		free(record->key);
	}
	if(record->value!=NULL){
		free(record->value);
	}
	free(record);
}

static void freeLRUCacheRecords(LRUCache* cache){
	LRUNode* node = cache->head;
	while ((node = node->next) != cache->head){
		Record *record = (Record *)node->value;
		freeRecord(record);
	}
}

HashEngine *makeHashEngine(const char *filename, uint32 hashMapCap, uint64 cacheCap){
	//创建文件并打开文件描述符
	int wfd = createHashFile(filename);
	int rfd = openHashFile(filename);
	if (wfd == -1 || rfd == -1)
	{
		return NULL;
	}
	//写入元数据
	writeMetadata(wfd);
	//创建对象
	HashEngine* engine = (HashEngine*)malloc(sizeof(HashEngine));
	engine->filename = (char *)malloc(sizeof(char) * (strlen(filename) + 1));
	strcpy(engine->filename, filename);
	engine->newFilename = NULL;
	engine->wfd = wfd;
	engine->rfd = rfd;
	engine->newrfd = -1;
	engine->idSeed = 1; //不能以0为起点
	engine->hashMap = makeHashMap(hashMapCap);
	engine->freezeHashMap = NULL;
	engine->readCache = makeLRUCache(cacheCap, 8); //每一个KEY对应一个唯一ID，从1开始
	engine->writeCache = makeLRUCache(cacheCap, 8);
	engine->freezeWriteCache = makeLRUCache(cacheCap, 8);
	engine->persistenceStatus = None; //没有进行持久化
	//初始化线程相关内容
	pthread_cond_init(&engine->statusCond, NULL);
	pthread_mutexattr_init(&engine->statusAttr);
	pthread_mutexattr_settype(&engine->statusAttr, PTHREAD_MUTEX_RECURSIVE_NP);
	pthread_mutex_init(&engine->statusMutex, &engine->statusAttr);
	return engine;
}

static void* setRecordLocationIdAs0(struct Entry * entry, void* args){
	((RecordLocation*) entry->value)->id = 0;
	return NULL;
}

HashEngine *loadHashEngine(const char *filename, uint32 hashMapCap, uint64 cacheCap){
	//创建文件并打开文件描述符
	int wfd = openHashFile(filename);
	int rfd = openHashFile(filename);
	if (wfd == -1 || rfd == -1)
	{
		return NULL;
	}
	//验证元数据
	if (!verifyMetadata(wfd))
	{
		return NULL;
	}
	//创建对象
	HashEngine *engine = (HashEngine *)malloc(sizeof(HashEngine));
	engine->filename = (char *)malloc(sizeof(char) * (strlen(filename) + 1));
	strcpy(engine->filename, filename);
	engine->newFilename = NULL;
	engine->wfd = wfd;
	engine->rfd = rfd;
	engine->newrfd = -1;
	engine->idSeed = 1; //不能以0为起点
	engine->hashMap = makeHashMap(hashMapCap);
	engine->freezeHashMap = NULL;
	engine->readCache = makeLRUCache(cacheCap, 8); //每一个KEY对应一个唯一ID，从1开始
	engine->writeCache = makeLRUCache(cacheCap, 8);
	engine->freezeWriteCache = makeLRUCache(cacheCap, 8);
	engine->persistenceStatus = None; //没有进行持久化
	//初始化线程相关内容
	pthread_cond_init(&engine->statusCond, NULL);
	pthread_mutexattr_init(&engine->statusAttr);
	pthread_mutexattr_settype(&engine->statusAttr, PTHREAD_MUTEX_RECURSIVE_NP);
	pthread_mutex_init(&engine->statusMutex, &engine->statusAttr);
	//读取数据文件，创建内存索引
	uint64 position = 8; //初始化为第一个数据段的位置
	uint64 fileSize = lseek(rfd, 0, SEEK_END);
	while(position<fileSize){
		Record* record = loadRecord(rfd, position, 1);
		RecordLocation* loaction = getHashMap(engine->hashMap, record->keyLen, record->key);
		if(loaction==NULL){
			loaction = makeRecordLocation(record->version, position); //先用id存放版本号
			putHashMap(engine->hashMap, record->keyLen, record->key, loaction);
		} else {
			if(loaction->id < record->version){
				loaction->position = position;
				loaction->id = record->version;
			}
		}
		freeRecord(record);
		position += 8 + 4 + 4 + record->keyLen + record->valueLen;
	}
	//恢复HashMap中的id为0
	foreachHashMap(engine->hashMap, setRecordLocationIdAs0, NULL);
	return engine;
}

void freeHashEngine(HashEngine* engine){
	startPersistenceThread(engine);
	pthread_join(engine->persistenceThread, NULL);
	free(engine->filename);
	if(engine->newFilename!=NULL){
		free(engine->newFilename);
	}
	close(engine->wfd);
	close(engine->rfd);
	if(engine->newrfd!=-1){
		close(engine->newrfd);
	}
	foreachHashMap(engine->hashMap, freeHashMapRecordLocation, NULL);
	freeHashMap(engine->hashMap);
	if(engine->freezeHashMap!=NULL){
		foreachHashMap(engine->freezeHashMap, freeHashMapRecordLocation, NULL);
		freeHashMap(engine->freezeHashMap);
	}
	freeLRUCacheRecords(engine->writeCache);
	freeLRUCache(engine->writeCache);
	freeLRUCacheRecords(engine->freezeWriteCache);
	freeLRUCache(engine->freezeWriteCache);
	free(engine);
}

/*****************************************************************************
 *通用函数：一些操作封装
 ******************************************************************************/

static void putToReadCache(HashEngine* engine, uint64 id, Record* record){
	Record* oldRecord = (Record*)putLRUCache(engine->readCache, (uint8*)&id, record);
	if(oldRecord!=NULL){
		//对淘汰的Record，将id清零
		RecordLocation* location = (RecordLocation*)getHashMap(engine->hashMap, oldRecord->keyLen, oldRecord->key);
		location->id = 0;
	}
}

static void putToWriteCache(HashEngine* engine, uint64 id, Record* record){

	if(engine->writeCache->size>=engine->writeCache->capacity){
		startPersistenceThread(engine);
	}
	putLRUCache(engine->writeCache, (uint8*)&id, record);
}

/*****************************************************************************
 *主要函数：增删改查
 ******************************************************************************/

int32 putHashEngine(HashEngine *engine, uint32 keyLen, uint8 *key, uint32 valueLen, uint8 *value){
	RecordLocation* location = (RecordLocation*) getHashMap(engine->hashMap, keyLen, key);
	Record* record = NULL;
	//不存在这个记录：创建
	if(location == NULL){
		//创建这个记录的位置
		location = makeRecordLocation(engine->idSeed++, 0);
		putHashMap(engine->hashMap, keyLen, key, location);
		//创建这个记录的内容
		record = makeRecord(1, keyLen, valueLen, key, value);
	}
	//存在这个记录：修改
	uint8 *copyValue = NULL;
	if (record == NULL){
		newAndCopyByteArray(&copyValue, value, valueLen);
	}
	//缓存中没有
	if(record == NULL && location->id==0 && location->position!=0){
		//从文件中读
		record = loadRecord(engine->rfd, location->position, 1);
		//修改其version和value
		record->version++;
		record->valueLen = valueLen;
		record->value = copyValue;
		//更新id
		location->id = engine->idSeed++;
	}
	//读缓存中有
	if (record==NULL && (record = (Record *)removeLRUCache(engine->readCache, (uint8*)&location->id)) != NULL)
	{
		free(record->value);
		//修改其version和value
		record->version++;
		record->valueLen = valueLen;
		record->value = copyValue;
	}
	//写缓存中有
	//防止出现在写writeCache时发生写缓存切换
	if(record==NULL){
		pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
		pthread_mutex_lock(&engine->statusMutex);
		if ((record = (Record *)removeLRUCache(engine->writeCache, (uint8 *)&location->id)) != NULL)
		{
			free(record->value);
			record->version++;
			record->valueLen = valueLen;
			record->value = copyValue;
		}
		pthread_mutex_unlock(&engine->statusMutex);
		pthread_cleanup_pop(0);
	}
	//读缓存、写缓存都没有，说明在freezewritecache中
	if(record==NULL){
		pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
		pthread_mutex_lock(&engine->statusMutex);
		if(engine->persistenceStatus == None){
			//出错
			return 0;
		} else if(engine->persistenceStatus == Doing){
			//另外的进程正在进行持久化
			if ((record = (Record *)getLRUCacheNoChange(engine->freezeWriteCache, (uint8 *)&location->id)) != NULL)
			{
				//重新创建一个记录
				record = makeRecord(record->version+1, record->keyLen, valueLen, record->key, value);
				//更新id
				location->id = engine->idSeed++;
			}
		} else if(engine->persistenceStatus == After){
			//另外的线程正在进行清理内存，wait
			pthread_cond_wait(&engine->statusCond, &engine->statusMutex);
		}
		pthread_mutex_unlock(&engine->statusMutex);
		pthread_cleanup_pop(0);
	}
	//将这个记录写入writecache
	if(record!=NULL){
		putToWriteCache(engine, location->id, record);
		//TODO 记录到重做日志
		return 1;
	}
	//说明持久化线程已经完成，递归调用
	return putHashEngine(engine, keyLen, key, valueLen, value);
}

Array getHashEngine(HashEngine *engine, uint32 keyLen, uint8 *key){
	Array arr;
	arr.array = NULL;
	arr.length = 0;

	RecordLocation *location = (RecordLocation *)getHashMap(engine->hashMap, keyLen, key);
	Record *record = NULL;

	if(location==NULL){
		return arr;
	}

	//不在内存中：从磁盘中读
	if(location->id==0){
		record = loadRecord(engine->rfd, location->position, 0);
		location->id = engine->idSeed++;
		putToReadCache(engine, location->id, record);
	}

	//从内存中读
	//从工作缓存中读
	if(record==NULL){
		record = (Record *)getLRUCache(engine->readCache, (uint8*)&location->id);
	}
	if(record==NULL){
		pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
		pthread_mutex_lock(&engine->statusMutex);
		record = (Record *)getLRUCache(engine->writeCache, (uint8*)&location->id);
		pthread_mutex_unlock(&engine->statusMutex);
		pthread_cleanup_pop(0);
	}
	pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
	pthread_mutex_lock(&engine->statusMutex);
	//说明在freezeWriteCache中
	if(record==NULL){
		if(engine->persistenceStatus == Doing){
			record = (Record *)getLRUCacheNoChange(engine->freezeWriteCache, (uint8 *)&location->id);
		} else if(engine->persistenceStatus == After){
			//另外的线程正在进行清理资源，wait
			pthread_cond_wait(&engine->statusCond, &engine->statusMutex);
		}
	}
	//以下代码也需要在临界区内，防止来自freezeWriteCache的record被释放
	if(record!=NULL){
		newAndCopyByteArray((uint8**)&arr.array, record->value, record->valueLen);
		arr.length = record->valueLen;
	}
	pthread_mutex_unlock(&engine->statusMutex);
	pthread_cleanup_pop(0);
	if(record!=NULL){
		return arr;
	}
	return getHashEngine(engine, keyLen, key);
}

Array removeHashEngine(HashEngine *engine, uint32 keyLen, uint8 *key){
	Array arr = getHashEngine(engine, keyLen, key);
	if (arr.length!=0){
		putHashEngine(engine, keyLen, key, 0, NULL);
	}
	return arr;
}

/*****************************************************************************
 *线程处理函数
 ******************************************************************************/

void flushHashEngine(HashEngine* engine){
	//Doing阶段：写入磁盘
	LRUCache *freezeCache = engine->freezeWriteCache;
	LRUNode *node = freezeCache->head;
	while ((node = node->next) != freezeCache->head){
		//TODO indexengine也有这个BUG这边在遍历，另一边在查询，破会了链表结构
		Record *record = (Record*)node->value;
		uint64 position = writeRecord(engine->wfd, record);
		RecordLocation* location = getHashMap(engine->hashMap, record->keyLen, record->key);
		location->position = position;
		//此时：id ！= 0 && position ！= 0
	}
	pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
	pthread_mutex_lock(&engine->statusMutex);
	engine->persistenceStatus = After;
	pthread_mutex_unlock(&engine->statusMutex);
	pthread_cleanup_pop(0);
	//After阶段：
	// 1、将RecordLocation.id置为0，表示内存中没有缓存
	// 2、清理内存
	while ((node = node->next) != freezeCache->head){
		Record *record = (Record *)node->value;
		RecordLocation *location = getHashMap(engine->hashMap, record->keyLen, record->key);
		if (*(uint64 *)node->key == location->id){
			//不相等说明，writecache中存在一个副本，不能清零，避免覆盖
			location->id = 0;
		}
		//此时：id == 0 && position ！= 0
		freeRecord(record);
	}
	clearLRUCache(freezeCache);
	pthread_cleanup_push((void *)pthread_mutex_unlock, &engine->statusMutex);
	pthread_mutex_lock(&engine->statusMutex);
	engine->persistenceStatus = None;
	pthread_cond_signal(&engine->statusCond); //唤醒等待中的线程
	pthread_mutex_unlock(&engine->statusMutex);
	pthread_cleanup_pop(0);
}