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
#include <unistd.h>

void demoPersistenceFunction(int fd, struct OperateTuple *op){
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
}

void testSynchronize(){
	char* filename = "test.redolog";
	unlink(filename);
	RedoLog* redoLog = makeRedoLog(filename, demoPersistenceFunction, synchronize, 0);
	// pthread_join(redoLog->persistenceThread, NULL);
	printf("===测试同步策略===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeOperateTuple(types[i], key, value);
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
	RedoLog* redoLog = makeRedoLog(filename, demoPersistenceFunction, definiteTime, 3100);
	printf("===测试定时策略===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeOperateTuple(types[i], key, value);
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
	RedoLog *redoLog = makeRedoLog(filename, demoPersistenceFunction, sizeThreshold, 3);
	printf("===测试阈值策略===\n");
	uint64 inputs[] =  {1,2,3,4,5,6,7,8};
	uint8 types[]  =   {1,2,3,1,2,3,3,3};
	uint64 *key, *value;
	for(int i=0; i<sizeof(inputs)/sizeof(inputs[0]); i++){
		newAndCopyByteArray((uint8 **)&key, (uint8 *)&inputs[i], sizeof(inputs[0]));
		newAndCopyByteArray((uint8 **)&value, (uint8 *)&inputs[i], sizeof(inputs[0]));
		OperateTuple *op = makeOperateTuple(types[i], key, value);
		printf("append操作type=%d key=%lld, value=%lld\n",
				types[i],
			   *key,
			   *value);
		appendRedoLog(redoLog, op);
	}
	freeRedoLog(redoLog);
}

TESTFUNC funcs[] = {
	testSynchronize,
	testDefiniteTime,
	testSizeThreshold,
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


