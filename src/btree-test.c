#include "btree.h"
#include <malloc.h>
#include <string.h>

// 为了方便测试，直接引入.c文件
#include "btree.c"

//=========test binarySearch=========
void testBinarySearch(){
	BTree *config = makeBTree(5, 1, 1);
	for(int i=0; i<4; i++){
		config->root->keys[i] = (uint8 *)malloc(sizeof(uint8));
	}
	config->root->keys[0][0] = 5;
	config->root->keys[1][0] = 5;
	config->root->keys[2][0] = 7;
	config->root->keys[3][0] = 9;
	config->root->size = 4;

	uint8 key=8;
	printf("%d\n", binarySearch(config->root, &key, config->keyLen));
	for(uint8 key=0; key<=10; key++){
		printf("key=%d; index=%d\n",key, binarySearch(config->root, &key, config->keyLen));
	}
}

//=========test searchBTree=========
void testSearchBTree(){
	BTree *config = makeBTree(5, 1, 1);

	for(int i=0; i<4; i++){
		config->root->keys[i] = (uint8 *)malloc(sizeof(uint8));
		config->root->values[i] = (uint8 *)malloc(sizeof(uint8));
	}

	config->root->keys[0][0] = 1;
	config->root->keys[1][0] = 5;
	config->root->keys[2][0] = 7;
	config->root->keys[3][0] = 9;

	config->root->values[0][0] = 1;
	config->root->values[1][0] = 5;
	config->root->values[2][0] = 7;
	config->root->values[3][0] = 9;
	config->root->size = 4;

	uint8 key=1;
	uint8 *value;
	value = searchBTree(config, &key);
	for(uint8 key=0; key<=10; key++){
		value = searchBTree(config, &key);
		if(value==NULL){
			printf("key=%d; value=NULL\n", key);
		} else {
			printf("key=%d; value=%d\n", key, *value);
		}
	}
}

//=========test insertBTree (2)=========
void testInsertBTree1(){
	BTree *config = makeBTree(3, 1, 1);
	uint8 data[] = {8,7,6,5,4,3,2,1,8};
	uint8 *key;
	uint8 *value;
	for(int i=0; i<sizeof(data); i++){
		key = malloc(sizeof(uint8) * config->keyLen);
		value = malloc(sizeof(uint8) * config->valueLen);
		*key = data[i];
		*value = data[i];
		insertBTree(config, key, value);
		value = searchBTree(config, key);
		if(value==NULL){
			printf("key=%d; value=NULL\n", *key);
		} else {
			printf("key=%d; value=%d\n", *key, *value);
		}
	}

	BTreeNode* p = config->sqt;
	while(p!=NULL){
		for(int i=0; i<p->size; i++){
			printf("%d\n", *p->keys[i]);
		}
		p = p->next;
	}
}

//=========test insertBTree (2)=========
void testInsertBTree2(){
	//多字节插入查找测试（8086 小端模式下，异常）
	BTree *config = makeBTree(3, 4, 4);
	uint32 data[] = {258,100, 6, 5, 256, 3, 2, 1};
	uint32 *key;
	uint32 *value;
	for (int i = 0; i < sizeof(data) / sizeof(uint32); i++)
	{
		key = malloc(sizeof(uint32) * config->keyLen);
		value = malloc(sizeof(uint32) * config->valueLen);
		*key = data[i];
		*value = data[i];
		insertBTree(config, (uint8 *)key, (uint8 *)value);
		value = (uint32*)searchBTree(config, (uint8 *)key);
		if (value == NULL)
		{
			printf("key=%d; value=NULL\n", *(uint32 *)key);
		}
		else
		{
			printf("key=%d; value=%d\n", *key, *value);
		}
	}

	BTreeNode *p = config->sqt;
	while (p != NULL)
	{
		for (int i = 0; i < p->size; i++)
		{
			printf("%d\n", *(uint32 *)p->keys[i]);
		}
		p = p->next;
	}
}

//=========test removeBTree=========
void testRemoveBTree(){
	BTree *config = makeBTree(3, 1, 1);
	uint8 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 7, 3, 5, 8, 8, 1, 2, 4, 6, 8};
	int8 ops[]   = {1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0};
	uint8 *key;
	uint8 *value;
	//插入测试数据
	for(int i=0; i<sizeof(data); i++){
		key = malloc(sizeof(uint8) * config->keyLen);
		value = malloc(sizeof(uint8) * config->valueLen);
		*key = data[i];
		*value = data[i];
		if(ops[i]){
			printf("insert key=%d ", *key);
			insertBTree(config, key, value);
		} else {
			printf("delete key=%d ", *key);
			removeBTree(config, key);
		}
		value = searchBTree(config, key);
		if(value==NULL){
			printf("in tree value=NULL\n");
		} else {
			printf("in tree value=%d\n", *value);
		}
	}

	BTreeNode *p = config->sqt;
	while (p != NULL)
	{
		for (int i = 0; i < p->size; i++)
		{
			printf("%d\n", *p->keys[i]);
		}
		p = p->next;
	}
}

#include <stdio.h>
int main(int argc, char const *argv[])
{
	printf("=========test binarySearch=========");
	testBinarySearch();
	printf("=========test searchBTree=========\n");
	testSearchBTree();
	printf("=========test insertBTree (1)=========\n");
	testInsertBTree1();
	printf("=========test insertBTree (2)=========\n");
	testInsertBTree2();
	printf("=========test removeBTree=========\n");
	testRemoveBTree();
	return 0;
}