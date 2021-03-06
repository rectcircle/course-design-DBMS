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
#include "redolog.h"
#include "indexengine.h"
#include <unistd.h>

void demoPersistenceFunction(struct RedoLog* redoLog, struct OperateTuple *op){
	if(op->type == OPERATETUPLE_TYPE_INSERT){
		printf("Persistence insert key=%lld, value=%lld\n", 
			*(uint64 *)op->objects->head->value, 
			*(uint64 *)op->objects->head->next->value);
	} else if(op->type == OPERATETUPLE_TYPE_REMOVE1){
		printf("Persistence remove1 key=%lld\n",
			   *(uint64 *)op->objects->head->value);
	} else if(op->type == OPERATETUPLE_TYPE_REMOVE2){
		printf("Persistence remove2 key=%lld, value=%lld\n",
			   *(uint64 *)op->objects->head->value,
			   *(uint64 *)op->objects->head->next->value);
	}
	usleep(1000*10);
	pthread_testcancel();
}

void demoFreeOperateTuple(OperateTuple* operateTuple){
	freeList(operateTuple->objects);
	free(operateTuple);
}

IndexEngine testEngine;

void init(){
	testEngine.treeMeta.keyLen = testEngine.treeMeta.valueLen = 8;
}

void testSynchronize(){
	char* filename = "test.redolog";
	unlink(filename);
	RedoLog *redoLog = makeRedoLog(filename, NULL, 0, (RedoPersistenceFunction)demoPersistenceFunction, (FreeOperateTupleFunction)demoFreeOperateTuple, synchronize, 0);
	// pthread_join(redoLog->persistenceThread, NULL);
	printf("===测试同步策略===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeIndexEngineOperateTuple(&testEngine ,types[i], key, value);
		printf("append操作type=%d key=%lld, value=%lld\n",
				types[i],
			   *key,
			   *value);
		appendRedoLog(redoLog, op);
	}
	freeRedoLog(redoLog);
}

void testDefiniteTime(){
	char* filename = "test.redolog";
	unlink(filename);
	uint64 operateListMaxSize = 100;
	RedoLog *redoLog = makeRedoLog(filename, NULL, operateListMaxSize, (RedoPersistenceFunction)demoPersistenceFunction, (FreeOperateTupleFunction)demoFreeOperateTuple, definiteTime, 3100);
	printf("===测试定时策略===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeIndexEngineOperateTuple(&testEngine, types[i], key, value);
		printf("append操作type=%d key=%lld, value=%lld\n",
				types[i],
			   *key,
			   *value);
		appendRedoLog(redoLog, op);
		sleep(1);
	}
	freeRedoLog(redoLog);
}

void testSizeThreshold(){
	char* filename = "test.redolog";
	unlink(filename);
	uint64 operateListMaxSize = 100;
	RedoLog *redoLog = makeRedoLog(filename, NULL, operateListMaxSize, (RedoPersistenceFunction)demoPersistenceFunction, (FreeOperateTupleFunction)demoFreeOperateTuple, sizeThreshold, 3);
	printf("===测试阈值策略===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeIndexEngineOperateTuple(&testEngine, types[i], key, value);
		printf("append操作type=%d key=%lld, value=%lld\n",
				types[i],
			   *key,
			   *value);
		appendRedoLog(redoLog, op);
	}
	freeRedoLog(redoLog);
}

#include "indexengine.h"

// void testPersistence(){
// 	char* filename = "test.redolog";
// 	unlink(filename);
// 	char *indexFilename = "test.idx";
// 	unlink(indexFilename);
// 	//度为6，每个缓存大小为3
// 	IndexEngine *engine = makeIndexEngine(indexFilename, 8, 8, 136, 0, 1024);
// 	uint64 operateListMaxSize = 100;
// 	RedoLog *redoLog = makeRedoLog(filename,(void*)engine,0, (RedoPersistenceFunction)indexEngineRedoLogPersistenceFunction, synchronize, 0);
// 	// RedoLog *redoLog = makeRedoLog(filename, (void *)engine, (RedoPersistenceFunction)indexEngineRedoLogPersistenceFunction, sizeThreshold, 3);

// 	printf("===测试索引引擎重做日志持久化===\n");
// 	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
// 	uint8 types[]  =   {1,2,3,1,2,3,3,3};
// 	uint64 *key, *value;
// 	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
// 		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
// 		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
// 		OperateTuple *op = makeIndexEngineOperateTuple(&testEngine ,types[i], key, value);
// 		printf("append操作type=%d key=%lld, value=%lld\n",
// 				types[i],
// 			   *key,
// 			   *value);
// 		appendRedoLog(redoLog, op);
// 	}
// 	freeRedoLog(redoLog);
// 	//重新打开
// 	redoLog = loadRedoLog(filename, (void *)engine,0, (RedoPersistenceFunction)indexEngineRedoLogPersistenceFunction, synchronize, 0);
// 	List* redoList = getIndexEngineOperateList(redoLog);
// 	freeRedoLog(redoLog);
// 	printf("读取到的redo列表为%d\n", redoList->length);
// 	ListNode* node = redoList->head;

// 	while(node!=NULL){
// 		OperateTuple* op = node->value;
// 		printf("opLen=%d type=%d op1=%lld\n", op->objects->length, op->type, *(uint64 *)op->objects->head->value);
// 		node = node->next;
// 	}
// 	freeList(redoList);
// }

void testOperateListMaxSize(){
	char* filename = "test.redolog";
	unlink(filename);
	uint64 operateListMaxSize = 3;
	RedoLog *redoLog = makeRedoLog(filename, NULL, operateListMaxSize, (RedoPersistenceFunction)demoPersistenceFunction, (FreeOperateTupleFunction)demoFreeOperateTuple, sizeThreshold, 3);
	printf("===最大操作链表尺寸阻塞===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeIndexEngineOperateTuple(&testEngine, types[i], key, value);
		printf("append操作type=%d key=%lld, value=%lld\n",
				types[i],
			   *key,
			   *value);
		appendRedoLog(redoLog, op);
	}
	freeRedoLog(redoLog);
}

void testForceFree(){
	char* filename = "test.redolog";
	unlink(filename);
	uint64 operateListMaxSize = 100;
	RedoLog *redoLog = makeRedoLog(filename, NULL, operateListMaxSize, (RedoPersistenceFunction)demoPersistenceFunction, (FreeOperateTupleFunction)demoFreeOperateTuple, sizeThreshold, 3);
	printf("===测试强制退出===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeIndexEngineOperateTuple(&testEngine, types[i], key, value);
		printf("append操作type=%d key=%lld, value=%lld\n",
				types[i],
			   *key,
			   *value);
		appendRedoLog(redoLog, op);
	}
	forceFreeRedoLog(redoLog);
}

TESTFUNC funcs[] = {
	init,
	testSynchronize,
	testDefiniteTime,
	testSizeThreshold,
	// testPersistence,
	testOperateListMaxSize,
	testForceFree,
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


