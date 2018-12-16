/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-lrucache.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "hashmap.h"
#include "test.h"
#include <stdio.h>

//=========test All=========

typedef uint8 TEST_TYPE;

void* outEntry(Entry* entry, void* args){
	printf("<%d,%d> ", *(TEST_TYPE *)entry->key, *(TEST_TYPE *)entry->value);
	return NULL;
}

void outMap(HashMap* map){
	foreachHashMap(map, (ForeachMapFunction) outEntry, NULL);
	printf("\n");
}


void testAll()
{
	HashMap* map = makeHashMap(16);
	TEST_TYPE input[] = {1, 2, 3, 4, 1, 2, 5, 3, 2, 4};
	TEST_TYPE op[]    = {1, 1, 1, 1, 0, 0, 1, 0, 0, 0};
	TEST_TYPE *key;
	TEST_TYPE *value;
	for(int i=0; i<sizeof(input)/sizeof(TEST_TYPE); i++){
		printf("第%d次操作：",i);
		key = input+i;
		value = input+i;
		if(op[i]){
			putHashMap(map, 1, key, value);
			printf("插入 %d 后，查询结果=%d，全部数据为：", input[i], *(TEST_TYPE *)getHashMap(map, 1, key));
			outMap(map);
		} else {
			void* result = removeHashMap(map, 1, key);
			if(result ==NULL){
				printf("删除 %d ，返回结果=NULL，", input[i]);
			} else {
				printf("删除 %d ，返回结果=%d，", input[i], *(TEST_TYPE *)result);
			}
			
			assertnull(getHashMap(map, 1, key), "在此查询结果应该为null");
			outMap(map);
		}
	}
	clearHashMap(map);
	outMap(map);
	freeHashMap(map);
}


int main(int argc, char const *argv[])
{
	printf("=========test All=========\n");
	launchTests(1, testAll);
	return 0;
}




