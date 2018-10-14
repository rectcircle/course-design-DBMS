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

void testReadWriteMeta(){
	printf("====测试读写元数据====\n");
	char* filename = "test.idx";
	unlink(filename);
	//od -t x1 test.idx
	IndexEngine *engine1 = makeIndexEngine(filename, 1, 1, 0, 0, 0);
	close(engine1->fd);
	IndexEngine *engine2 = loadIndexEngine(filename, 0);
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
	// //度为6，每个缓存大小为3
	// IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024);
	//度为6，每个缓存大小为7
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 2048);
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
	//度为6，每个缓存大小为3
	IndexEngine *engine = makeIndexEngine(filename, 8, 8, 136, 0, 1024);
	uint64 inputs[] = {1, 2, 3, 4, 5, 6, 7};
	List* list = searchIndexEngine(engine, (uint8*)&(inputs[0]));
	printf("列表长度=%d\n",list->length);
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		insertIndexEngine(engine, (uint8*)&(inputs[i]),(uint8*)&(inputs[i]));
		list = searchIndexEngine(engine, (uint8 *)&(inputs[i]));
		printf("search key=%lld, value=%lld, length=%d\n", inputs[i], *(uint64*)list->head->value, list->length);
	}
	pthread_join(engine->cache.persistenceThread, NULL);
	unlink(filename);
}

TESTFUNC funcs[] = {
	testReadWriteMeta,
	testInsertAndSearch,
	testPersistenceThread
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


