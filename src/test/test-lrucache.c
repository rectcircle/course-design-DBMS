/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-lrucache.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "lrucache.h"
#include "test.h"

//=========test All=========

void outList(LRUCache* cache){
	LRUNode* p = cache->head->next;
	while(p!=cache->head){
		printf("<%d,%d> ", *(uint8*)p->key, *(uint8*)p->value);
		p = p->next;
	}
	printf("\n");
}

void outEliminateData(uint32 keyLen, uint8*key , uint8* value){
	printf("<%d,%d>将要被淘汰 ", *(uint8 *)key, *(uint8 *)value);
}

void testAll()
{
	LRUCache* cache = makeLRUCache(3, 1);
	printf("HashTable桶数组大小=%d\n", cache->bucketCapacity);
	assertuint(4, cache->bucketCapacity, "桶数组大小应该等于4");
	assertuint(16, makeLRUCache(8, 1)->bucketCapacity, "桶数组大小应该等于16");
	typedef uint8 TEST_TYPE;
	TEST_TYPE input[] = {1, 2, 3, 4, 1, 2, 5, 3, 2, 4};
	TEST_TYPE op[]    = {1, 1, 1, 1, 0, 0, 1, 0, 0, 0};
	TEST_TYPE output[]= {0, 0, 0, 0, 0, 2, 0, 0, 2, 4};
	TEST_TYPE *key;
	TEST_TYPE *value;
	for(int i=0; i<sizeof(input)/sizeof(TEST_TYPE); i++ ){
		printf("第%d次操作：",i);
		if(op[i]){
			key = malloc(sizeof(TEST_TYPE));
			value = malloc(sizeof(TEST_TYPE));
			*key = input[i];
			*value = input[i];
			putLRUCacheWithHook(cache, key, value, outEliminateData);
			printf("插入 %d 后List：", input[i]);
			outList(cache);
		} else {
			key = malloc(sizeof(TEST_TYPE));
			*key = input[i];
			value = getLRUCache(cache,key);
			printf("查询 %d 后List：", input[i]);
			outList(cache);
			if(value==NULL){
				printf("value=NULL, ");
			} else {
				printf("value=%d, ", *(TEST_TYPE*)value);
			}
			if(output[i]==0){
				printf("应该为NULL\n");
				assertnull(value, "测试LUR get");
			} else {
				printf("应该为%d\n", output[i]);
				assertuchar(output[i], *value, "测试LUR get");
			}

		}
	}
}

#include <stdio.h>
int main(int argc, char const *argv[])
{
	printf("=========test All=========\n");
	testAll();
	return 0;
}




