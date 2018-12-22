/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-lrucache.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "hashengine.h"
#include "test.h"
#include "util.h"
#include <stdio.h>
#include <unistd.h>
#include <time.h>

void testInMemery(){
	printf("====测试在内存中增删改查====\n");
	char *filename = "test.hashengine";
	unlink(filename);
	HashEngine* engine = makeHashEngine(filename, 10, 10);
	//测试key
	uint8 keys[]    = {1, 2, 3, 4, 1, 2, 2, 2, 4, 4};
	uint64 values[] = {1, 2, 3, 4, 1, 2, 0, 0, 6, 6};
	uint8 ops[]     = {0, 0, 0, 0, 1, 2, 1, 2, 0, 1}; //0-删除，1-查找，2-删除
	for(int i = 0; i<sizeof(ops)/sizeof(ops[0]); i++){
		if(ops[i]==0){
			putHashEngine(engine, 1, (uint8 *)&keys[i], 8, (uint8 *)&values[i]);
			printf("<%d, %lld>已经插入\n", keys[i], values[i]);
		} else if(ops[i]==1){
			Array arr = getHashEngine(engine, 1, (uint8 *)&keys[i]);
			if(values[i]==0){
				printf("查询key=%d结果len应该为0， 实际为len=%d\n", keys[i], arr.length);
				assertuint(0, arr.length, "查询测试，返回数组长度应该为0");
			} else {
				assertulonglong(values[i], *(uint64*)arr.array, "查询测试应该有结果");
				printf("查询结果<%d, %lld>\n", keys[i], *(uint64 *)arr.array);
			}
		} else {
			Array arr = removeHashEngine(engine, 1, (uint8 *)&keys[i]);
			if(values[i]==0){
				printf("删除key=%d返回结果len应该为0， 实际为len=%d\n",keys[i], arr.length);
				assertuint(0, arr.length, "删除测试，返回数组长度应该为0");
			} else {
				assertulonglong(values[i], *(uint64*)arr.array, "删除测试应该有结果");
				printf("删除kv为<%d, %lld>\n", keys[i], *(uint64 *)arr.array);
			}
		}
	}
	freeHashEngine(engine);
}

void testInDisk(){
	printf("====测试在磁盘中增删改查====\n");
	char *filename = "test.hashengine";
	unlink(filename);
	HashEngine* engine = makeHashEngine(filename, 10, 3);
	//测试key
	uint8 keys[]    = {1, 2, 3, 4, 1, 2, 2, 2, 4, 4, 1, 5, 6, 7, 8, 9, 2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
	uint64 values[] = {1, 2, 3, 4, 1, 2, 0, 0, 6, 6, 1, 5, 6, 7, 8, 9, 2, 1, 2, 3, 6, 5, 6, 7, 8, 9, 0};
	uint8 ops[]     = {0, 0, 0, 0, 1, 2, 1, 2, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; //0-删除，1-查找，2-删除
	for(int i = 0; i<sizeof(ops)/sizeof(ops[0]); i++){
		if(ops[i]==0){
			putHashEngine(engine, 1, (uint8 *)&keys[i], 8, (uint8 *)&values[i]);
			printf("<%d, %lld>已经插入\n", keys[i], values[i]);
		} else if(ops[i]==1){
			Array arr = getHashEngine(engine, 1, (uint8 *)&keys[i]);
			if(values[i]==0){
				printf("查询key=%d结果len应该为0， 实际为len=%d\n", keys[i], arr.length);
				assertuint(0, arr.length, "查询测试，返回数组长度应该为0");
			} else {
				assertulonglong(values[i], *(uint64*)arr.array, "查询测试应该有结果");
				printf("查询结果<%d, %lld>\n", keys[i], *(uint64 *)arr.array);
			}
		} else {
			Array arr = removeHashEngine(engine, 1, (uint8 *)&keys[i]);
			if(values[i]==0){
				printf("删除key=%d返回结果len应该为0， 实际为len=%d\n",keys[i], arr.length);
				assertuint(0, arr.length, "删除测试，返回数组长度应该为0");
			} else {
				assertulonglong(values[i], *(uint64*)arr.array, "删除测试应该有结果");
				printf("删除kv为<%d, %lld>\n", keys[i], *(uint64 *)arr.array);
			}
		}
	}
	freeHashEngine(engine);
}

void testLoadEngine(){
	testInDisk();
	printf("====测试在重新加载====\n");
	char *filename = "test.hashengine";
	HashEngine* engine = loadHashEngine(filename, 10, 3);
	//测试key
	uint8 keys[]    = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
	uint64 values[] = {1, 2, 3, 6, 5, 6, 7, 8, 9, 0};
	uint8 ops[]     = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; //0-删除，1-查找，2-删除
	for(int i = 0; i<sizeof(ops)/sizeof(ops[0]); i++){
		if(ops[i]==0){
			putHashEngine(engine, 1, (uint8 *)&keys[i], 8, (uint8 *)&values[i]);
			printf("<%d, %lld>已经插入\n", keys[i], values[i]);
		} else if(ops[i]==1){
			Array arr = getHashEngine(engine, 1, (uint8 *)&keys[i]);
			if(values[i]==0){
				printf("查询key=%d结果len应该为0， 实际为len=%d\n", keys[i], arr.length);
				assertuint(0, arr.length, "查询测试，返回数组长度应该为0");
			} else {
				assertulonglong(values[i], *(uint64*)arr.array, "查询测试应该有结果");
				printf("查询结果<%d, %lld>\n", keys[i], *(uint64 *)arr.array);
			}
		} else {
			Array arr = removeHashEngine(engine, 1, (uint8 *)&keys[i]);
			if(values[i]==0){
				printf("删除key=%d返回结果len应该为0， 实际为len=%d\n",keys[i], arr.length);
				assertuint(0, arr.length, "删除测试，返回数组长度应该为0");
			} else {
				assertulonglong(values[i], *(uint64*)arr.array, "删除测试应该有结果");
				printf("删除kv为<%d, %lld>\n", keys[i], *(uint64 *)arr.array);
			}
		}
	}
	freeHashEngine(engine);
}

void testRandomOps(){
	printf("====测试大量随机插入查找====\n");
	char *filename = "test.hashengine";
	unlink(filename);
	HashEngine *engine = makeHashEngine(filename, 1000, 3);
	int seed = (int)time(0);
	// int seed = 1545464059;
	printf("seed=%d\n", seed);
	srand(seed);

	const int MAX_TEST = 1000;

	uint32 keys[MAX_TEST];
	uint32 values[MAX_TEST];
	uint32 ops[MAX_TEST];

	for(int i=0; i<MAX_TEST; i++){
		keys[i] = rand() % MAX_TEST;
		values[i] = rand() % MAX_TEST;
		ops[i] = rand() % 2;
	}

	for(int i=0; i<MAX_TEST; i++){
		uint32 key = keys[i];
		uint32 value = values[i];
		uint32 op = ops[i];
		if(op){
			putHashEngine(engine, 4, (uint8 *)&key, 4, (uint8 *)&value);
			Array arr = getHashEngine(engine, 4, (uint8 *)&key);
			printf("id=%d 插入 <key, value> = <%u, %u>\n", i, key, value);
			assertuint(value, *(uint32 *)arr.array, "查询测试应该有结果");
		} else {
			removeHashEngine(engine, 4, (uint8 *)&key);
			Array arr = getHashEngine(engine, 4, (uint8 *)&key);
			printf("id=%d 删除 <key, ?> = <%u, ?>\n", i, key);
			assertuint(0, arr.length, "删除测试，返回数组长度应该为0");
		}
	}
	freeHashEngine(engine);
}

TESTFUNC funcs[] = {
	testInMemery,
	testInDisk,
	testLoadEngine,
	testRandomOps,
};

int main(int argc, char const *argv[])
{
	printf("=========test All=========\n");
	launchTestArray(sizeof(funcs)/sizeof(funcs[0]), funcs);
	return 0;
}




