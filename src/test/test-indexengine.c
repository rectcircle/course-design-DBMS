/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-test.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "test.h"
#include "util.h"
#include "indexengine.h"
#include <time.h>

static uint64 operateListMaxSize = 10000;
static enum RedoFlushStrategy flushStrategy = sizeThreshold;
static uint64 flushStrategyArg = 10000;

static void clearRedoLogFile(char *indexFliename){
	char *filename = malloc(strlen(indexFliename) + 30);
	for(uint64 i=2; i<10; i++){
		sprintf(filename, "%s_0x%016llx.redolog", indexFliename, i);
		unlink(filename);
	}
}

void testReadWriteMeta(){
	printf("====测试读写元数据====\n");
	char* filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	//od -t x1 test.idx
	IndexEngine *engine1 = makeIndexEngine(filename, 1, 1, 0, 0, 0,operateListMaxSize,flushStrategy, flushStrategyArg);
	close(engine1->rfd);
	close(engine1->wfd);
	IndexEngine *engine2 = loadIndexEngine(filename, 0, operateListMaxSize, flushStrategy, flushStrategyArg);
	printf("count=%lld %lld\n", engine1->count, engine2->count);
	assertlonglong(engine1->count, engine2->count, "count");
	printf("filename=%s %s\n", engine1->filename, engine2->filename);
	assertstring(engine1->filename, engine2->filename, "filename");
	printf("flag=%d %d\n", engine1->flag, engine2->flag);
	assertint(engine1->flag, engine2->flag, "flag");
	printf("magic=%x %x\n", engine1->magic, engine2->magic);
	assertint(engine1->magic, engine2->magic, "magic");
	printf("nextPageId=%lld %lld\n", engine1->nextPageId, engine2->nextPageId);
	assertlonglong(engine1->nextPageId, engine2->nextPageId, "nextPageId");
	printf("pageSize=%d %d\n", engine1->pageSize, engine2->pageSize);
	assertint(engine1->pageSize, engine2->pageSize, "pageSize");
	printf("usedPageCnt=%lld %lld\n", engine1->usedPageCnt, engine2->usedPageCnt);
	assertlonglong(engine1->usedPageCnt, engine2->usedPageCnt, "usedPageCnt");
	printf("version=%d %d\n", engine1->version, engine2->version);
	assertint(engine1->version, engine2->version, "version");
	printf("degree=%d %d\n", engine1->treeMeta.degree, engine2->treeMeta.degree);
	assertint(engine1->treeMeta.degree, engine2->treeMeta.degree, "treeMeta.degree");
	printf("depth=%d %d\n", engine1->treeMeta.depth, engine2->treeMeta.depth);
	assertint(engine1->treeMeta.depth, engine2->treeMeta.depth, "treeMeta.depth");
	printf("isUnique=%d %d\n", (int)engine1->treeMeta.isUnique, (int)engine2->treeMeta.isUnique);
	assertbool(engine1->treeMeta.isUnique, engine2->treeMeta.isUnique, "treeMeta.isUnique");
	printf("keyLen=%d %d\n", engine1->treeMeta.keyLen, engine2->treeMeta.keyLen);
	assertint(engine1->treeMeta.keyLen, engine2->treeMeta.keyLen, "treeMeta.keyLen");
	printf("valueLen=%d %d\n", engine1->treeMeta.valueLen, engine2->treeMeta.valueLen);
	assertint(engine1->treeMeta.valueLen, engine2->treeMeta.valueLen, "treeMeta.valueLen");
	printf("root=%lld %lld\n", engine1->treeMeta.root, engine2->treeMeta.root);
	assertlonglong(engine1->treeMeta.root, engine2->treeMeta.root, "treeMeta.root");
	printf("sqt=%lld %lld\n", engine1->treeMeta.sqt, engine2->treeMeta.sqt);
	assertlonglong(engine1->treeMeta.sqt, engine2->treeMeta.sqt, "treeMeta.sqt");
	printf("nextNodeVersion=%lld %lld\n", engine1->nextNodeVersion, engine2->nextNodeVersion);
	assertlonglong(engine1->nextNodeVersion, engine2->nextNodeVersion, "nextNodeVersion");
	unlink(filename);
}

void testInsertAndSearch(){
	printf("====测试插入查询====\n");
	char* filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	// //度为6，每个缓存大小为3
	// IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024);
	//度为6，每个缓存大小为7
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 2048, operateListMaxSize, flushStrategy, flushStrategyArg);
	uint64 inputs[] = {1, 1, 3, 3, 5, 6, 7};
	List* list = searchIndexEngine(engine, (uint8*)&(inputs[0]));
	printf("列表长度=%d\n",list->length);
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		insertIndexEngine(engine, (uint8*)&(inputs[i]),(uint8*)&(inputs[i]));
		list = searchIndexEngine(engine, (uint8 *)&(inputs[i]));
		printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
	}
	unlink(filename);
}

void testPersistenceThread(){
	printf("====测试持久化====\n");
	char* filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	//度为6，每个缓存大小为3
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024, operateListMaxSize, flushStrategy, flushStrategyArg);
	uint64 inputs[] = {1, 2, 3, 4, 5, 6, 7};
	List* list = searchIndexEngine(engine, (uint8*)&(inputs[0]));
	freeList(list);
	printf("列表长度=%d\n",list->length);
	printf("插入+查找====\n");
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		uint64 key = htonll(inputs[i]);
		insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(inputs[i]));
		list = searchIndexEngine(engine, (uint8 *)&key);
		printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
		freeList(list);
	}
	printf("持久化+查找====\n");
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		uint64 key = htonll(inputs[i]);
		list = searchIndexEngine(engine, (uint8 *)&key);
		printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
		freeList(list);
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
	printf("持久化后读====\n");
	IndexEngine *engine1 = loadIndexEngine(filename, 1024, operateListMaxSize, flushStrategy, flushStrategyArg);
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		uint64 key = htonll(inputs[i]);
		list = searchIndexEngine(engine1, (uint8 *)&key);
		printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
		freeList(list);
	}
	printf("插入+查找====\n");
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		uint64 key = htonll(inputs[i]);
		insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(inputs[i]));
		list = searchIndexEngine(engine, (uint8 *)&key);
		printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
		freeList(list);
	}
	printf("大量数据插入+查找====\n");
	uint64 data;
	for(int i=1000; i>=0; i--){
		data = i+8;
		uint64 key = htonll(data);
		insertIndexEngine(engine, (uint8*)&key,(uint8*)&data);
		list = searchIndexEngine(engine, (uint8 *)&key);
		printf("search key=%lld, value=%lld, length=%d\n", data, *(uint64*)list->head->value, list->length);
		freeList(list);
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
	// srand((int)time(0));
	srand(0);
	printf("大量随机数据插入+查找====\n");
	for(int i=0; i<1000; i++){
		data = rand();
		uint64 key = htonll(data);
		int rows = insertIndexEngine(engine, (uint8*)&key,(uint8*)&data);
		printf("rows=%d\n", rows);
		list = searchIndexEngine(engine, (uint8 *)&key);
		printf("search key=%lld, value=%lld, length=%d\n", data, *(uint64*)list->head->value, list->length);
		freeList(list);
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
	// unlink(filename);
}

void persistenceException(int expId){
	char *filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	//度为6，每个缓存大小为3
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024, 0, synchronize, 0);
	uint64 inputs[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
	persistenceExceptionId = 0;
	for(int i=0; i<7; i++){
		uint64 key = htonll(inputs[i]);
		insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(inputs[i]));
		// list = searchIndexEngine(engine, (uint8 *)&key);
		// printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
		// freeList(list);
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
	persistenceExceptionId = expId;
	for(int i=7; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		uint64 key = htonll(inputs[i]);
		insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(inputs[i]));
		// list = searchIndexEngine(engine, (uint8 *)&key);
		// printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
		// freeList(list);
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
	persistenceExceptionId = 0;
}
void testPersistenceException(){
	printf("====测试持久化断电情况：仅仅丢失最后一次持久化的数据====\n");
	char *filename = "test.idx";
	IndexEngine *engine;
	uint64 inputs[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
	List *list;
	for(int i=0; i<13; i++){
		printf("异常位置为：%d\n", i);
		persistenceException(i);
		engine = loadIndexEngine(filename, 1024, operateListMaxSize, flushStrategy, flushStrategyArg);
		for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
			uint64 key = htonll(inputs[i]);
			list = searchIndexEngine(engine, (uint8 *)&key);
			if(list->length==0){
				printf("search key=%lld, value=NULL, length=%d\n", inputs[i], list->length);
			} else {
				printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64 *)list->head->value, list->length);
			}
			freeList(list);
		}
		sleep(1);
	}
}

void testRemove1(){
	printf("====测试删除1====\n");
	char *filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	//度为6，每个缓存大小为3
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024, operateListMaxSize, flushStrategy, flushStrategyArg);
	uint64 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 7, 3, 5, 8, 8, 1, 2, 4, 6, 8};
	int8 ops[]   = {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0};
	//插入测试数据
	for(int i=0; i<sizeof(data)/sizeof(data[0]); i++){
		uint64 key = htonll(data[i]);
		uint64 value = data[i];
		if(ops[i]){
			printf("insert key=%lld ", value);
			insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(value));
		} else {
			int32 cnt = removeIndexEngine(engine, (uint8 *)&key, NULL);
			printf("delete key=%lld cnt=%d ", value, cnt);
			pthread_join(*engine->cache.persistenceThread, NULL);
		}
		List* list = searchIndexEngine(engine, (uint8 *)&key);
		if(list->length==0){
			printf("in tree value=NULL\n");
		} else {
			printf("in tree value=%lld\n", *(uint64*)list->head->value);
		}
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
}

void testRemove2(){
	printf("====测试删除2====\n");
	char *filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	//度为6，每个缓存大小为3
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024, operateListMaxSize, flushStrategy, flushStrategyArg);
	uint64 data[] = {1, 1, 1, 1, 1, 1, 1, 1, 1};
	int8 ops[]    = {1, 1, 1, 1, 1, 1, 1, 1, 0};
	//插入测试数据
	for(int i=0; i<sizeof(data)/sizeof(data[0]); i++){
		uint64 key = htonll(data[i]);
		uint64 value = data[i];
		if(ops[i]){
			printf("insert key=%lld ", value);
			insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(value));
		} else {
			int32 cnt = removeIndexEngine(engine, (uint8 *)&key, NULL);
			printf("delete key=%lld cnt=%d ", value, cnt);
			pthread_join(*engine->cache.persistenceThread, NULL);
		}
		List* list = searchIndexEngine(engine, (uint8 *)&key);
		if(list->length==0){
			printf("in tree value=NULL\n");
		} else {
			printf("in tree value=%lld\n", *(uint64*)list->head->value);
		}
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
}

void testRemove3(){
	printf("====测试删除3====\n");
	char *filename = "test.idx";
	unlink(filename);
	clearRedoLogFile(filename);
	//度为6，每个缓存大小为3
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024, operateListMaxSize, flushStrategy, flushStrategyArg);
	uint64 keys[]   = {1, 1, 1, 1, 1, 1, 1, 1, 1};
	uint64 values[] = {1, 2, 3, 4, 5, 6, 7, 8, 3};
	int8 ops[]      = {1, 1, 1, 1, 1, 1, 1, 1, 0};
	//插入测试数据
	for(int i=0; i<sizeof(keys)/sizeof(keys[0]); i++){
		uint64 key = htonll(keys[i]);
		uint64 value = values[i];
		if(ops[i]){
			printf("insert key=%lld ", value);
			insertIndexEngine(engine, (uint8 *)&key, (uint8 *)&(value));
		} else {
			int32 cnt = removeIndexEngine(engine, (uint8 *)&key, (uint8 *)&value);
			printf("delete key=%lld cnt=%d ", value, cnt);
			pthread_join(*engine->cache.persistenceThread, NULL);
		}
		List* list = searchIndexEngine(engine, (uint8 *)&key);
		if(list->length==0){
			printf("in tree value=NULL\n");
		} else {
			printf("in tree value=%lld\n", *(uint64*)list->head->value);
		}
	}
	pthread_join(*engine->cache.persistenceThread, NULL);
}

TESTFUNC funcs[] = {
	// testReadWriteMeta,
	// testInsertAndSearch,
	// // testPersistenceThread,
	testPersistenceException,
	// testRemove1,
	// testRemove2,
	// testRemove3,
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


