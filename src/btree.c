#include "btree.h"
#include <malloc.h>
#include <string.h>

//创建一个可用节点
BTreeNode *makeBTreeNode(BTree *config, uint64 pageId, int8 isLeaf){
	//分配内存
	BTreeNode *node = (BTreeNode *)malloc(sizeof(BTreeNode));
	//初始化页面号
	node->pageId=pageId;
	//初始化keys
	node->keys = (uint8 **)malloc(sizeof(uint8*) * (config->degree+1));
	//初始化值和孩子节点
	if(isLeaf){ //如果是叶子节点
		node->values = (uint8 **)malloc(sizeof(uint8*) * (config->degree+1));
		node->children = NULL;
	} else { //如果不是叶子节点
		node->values = NULL;
		node->children = (BTreeNode **)malloc(sizeof(BTreeNode *) * (config->degree+1));
	}
	node->size = 0;
	node->next = NULL;
	return node;
}

//创建一个B+树，并创建根节点
BTree *makeBTree(uint32 degree, uint32 keyLen, uint32 valueLen){
	BTree *config = (BTree *)malloc(sizeof(BTree));
	config->depth = 1;
	config->degree = degree;
	config->keyLen = keyLen;
	config->valueLen = valueLen;
	BTreeNode* root = makeBTreeNode(config, 0, 1);
	config->root = root;
	config->sqt = root;
	return config;
}

//字节数组比较
//返回 <0 a<b; =0 a=b; >0 a>b
static int32 byteArrayCompare(uint32 len, uint8* a, uint8* b){
	for(uint32 i=0; i<len; i++){
		if(a[i]!=b[i]){
			return a[i]-b[i];
		}
	}
	return 0;
}

//二分查找，确定其索引
//从数组中找到小于等于key的第一个元素的下标，若不存在返回-1
//例如 5 5 7 9
//input 1 2 5 6 7 8 9 10
//output -1 -1 0 1 2 2 3 3
static int32 binarySearch(BTreeNode* root, uint8* key, uint32 keyLen){
	if(root->size==0){
		return -1;
	}
	int32 left = 0, right = root->size-1, mid;
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

//从BTree中查找一条记录
uint8 *searchBTree(BTree *config, uint8 *key){
	BTreeNode* root = config->root;
	int32 level = 1;
	while(level<config->depth){ //一直查找到叶子节点
		int32 index = binarySearch(root, key, config->keyLen);
		if(index<0){
			return NULL;
		}
		root = root->children[index];
		level++;
	}
	int32 idx = binarySearch(root, key, config->keyLen);
	if(idx==-1){
		return NULL;
	}
	if(0==byteArrayCompare(config->keyLen, root->keys[idx] , key)){
		return root->values[idx];
	}
	return NULL;
}

//在数组array的第index位置插入一个元素
//0表示成功；-1表示无法插入
static int32 insertToArray(void** array, uint32 len, uint32 index, void* value){
	if(index<0 || index>=len){
		return -1;
	}
	for(int32 i=len-1; i>index; i--){
		array[i]=array[i-1];
	}
	array[index] = value;
	return 0;
}

//从数组array删除第index位置的元素
//0表示成功；-1表示无法删除
static int32 deleteFromArray(void** array, uint32 len, uint32 index){
	if(index<0 || index>=len){
		return -1;
	}
	free(array[index]);
	for(int32 i=index; i<len-1; i++){
		array[i]=array[i+1];
	}
	return 0;
}

//根据现有叶子节点分裂成两个节点，返回新创建的节点
static BTreeNode *splitLeafBTreeNode(BTree *config, BTreeNode *nowNode){
	BTreeNode* newNode = makeBTreeNode(config, 0, 1);
	int len = nowNode->size / 2;
	memcpy(newNode->keys, nowNode->keys + len, sizeof(void *) * (nowNode->size - len));
	memcpy(newNode->values, nowNode->values + len, sizeof(void *) * (nowNode->size - len));
	newNode->size = nowNode->size - len;
	nowNode->size = len;
	BTreeNode* tmp = nowNode->next;
	nowNode->next = newNode;
	newNode->next = tmp;
	return newNode;
}

//根据现有非叶子节点分裂成两个节点，返回新创建的节点
static BTreeNode *splitNonLeafBTreeNode(BTree *config, BTreeNode *nowNode){
	BTreeNode* newNode = makeBTreeNode(config, 0, 0);
	int len = nowNode->size / 2;
	//拷贝key
	memcpy(newNode->keys, nowNode->keys+len+1, sizeof(void*) * (nowNode->size-len-1));
	//将要提升的中间的key备份在此
	newNode->keys[nowNode->size-len-1] =  nowNode->keys[len]; 
	//拷贝孩子指针
	memcpy(newNode->children, nowNode->children+len+1, sizeof(void*) * (nowNode->size-len));
	newNode->size = nowNode->size-len-1;
	nowNode->size = len;
	BTreeNode *tmp = nowNode->next;
	nowNode->next = newNode;
	newNode->next = tmp;
	return newNode;
}

static BTreeNode *splitBTreeNode(BTree *config, BTreeNode *nowNode, int8 isLeaf){
	BTreeNode *newNode = makeBTreeNode(config, 0, isLeaf);
	int len = nowNode->size / 2; //此时size == degree+1
	memcpy(newNode->keys, nowNode->keys+len, sizeof(void *) * (nowNode->size - len));
	if(isLeaf){
		memcpy(newNode->values, nowNode->values+len, sizeof(void*) * (nowNode->size-len));
	} else {
		memcpy(newNode->children, nowNode->children+len, sizeof(void*) * (nowNode->size-len));
	}
	newNode->size = nowNode->size-len;
	nowNode->size = len;
	BTreeNode *tmp = nowNode->next;
	nowNode->next = newNode;
	newNode->next = tmp;
	return newNode;
}

//递归进行插入及树重建
static BTreeNode* insertTo(BTree *config, BTreeNode* now, uint8 *key, uint8 *value, int32 level){
	int32 index = binarySearch(now, key, config->keyLen);
	//是叶子节点
	if(level == config->depth){
		insertToArray((void **)now->keys, config->degree+1, index+1, (void *)key);
		insertToArray((void **)now->values, config->degree+1, index+1, (void *)value);
		now->size++;
		if(now->size<=config->degree){ //未满
			return NULL;
		}
		//已满
		BTreeNode *newNode = splitBTreeNode(config, now, 1);
		return newNode;
	} 
	//不是叶子节点
	if(index==-1){
		now->keys[0] = key;
		index++;
	}
	BTreeNode *next = now->children[index];
	BTreeNode *result = insertTo(config, next, key, value, level+1);

	//非叶子节点后续处理
	if(result==NULL){
		return NULL;
	}
	uint8 *childKey = result->keys[0];
	insertToArray((void **)now->keys, config->degree+1, index+1, (void *)childKey);
	insertToArray((void **)now->children, config->degree+1, index+1, (void *)result);

	now->size++;
	if(now->size<=config->degree){ //未满
		return NULL;
	}
	//已满
	BTreeNode *newNode = splitBTreeNode(config, now, 0);
	return newNode;
}

//向BTree添加添加一条记录
//返回0 插入成功，返回其他表示插入失败
int32 insertBTree(BTree *config, uint8 *key, uint8 *value){
	//违反唯一约束
	if(config->isUnique && searchBTree(config, key)!=NULL){ 
		return -1;
	}
	BTreeNode *newChild = insertTo(config, config->root, key, value, 1);
	if(newChild!=NULL){
		BTreeNode *newRoot = makeBTreeNode(config, 0, 0);
		newRoot->keys[0] = config->root->keys[0];
		newRoot->keys[1] = newChild->keys[0];
		newRoot->children[0] = config->root;
		newRoot->children[1] = newChild;
		newRoot->size = 2;
		config->root = newRoot;
		config->depth++;
	}
	return 0;
}

//根据现有叶子节点分裂成两个节点，返回新创建的节点
static BTreeNode *mergeLeafBTreeNode(BTree *config, BTreeNode *nowNode, int32 index){
}

//根据现有非叶子节点分裂成两个节点，返回新创建的节点
static BTreeNode *mergeNonLeafBTreeNode(BTree *config, BTreeNode *nowNode, int32 index){
}

static uint8* deleteFrom(BTree* config, BTreeNode* now, uint8 *key){
	//是叶子节点
	if (now->children == NULL){
		int32 index = binarySearch(now, key, config->keyLen);
		//找不到该元素
		if(byteArrayCompare(config->keyLen, now->keys[index], key)==0){
			return now->keys[0];
		}
		deleteFromArray((void**)now->keys, now->size, index);

		if (--now->size){
			return now->keys[0];
		} else{
			return NULL; //删空了
		}
	}
	//非叶子节点
	int32 index = binarySearch(now, key, config->keyLen);
	BTreeNode *next = now->children[index];
	uint8* childFirstKey = deleteFrom(config, next, key);
	//更新当前节点key
	if(next->size>0 && index>0){
		if (childFirstKey){
			now->keys[index - 1] = childFirstKey;
		} else {
			now->keys[index - 1] = childFirstKey;
		}
	}
	//任然满足B+树的定义
	if (next->size>=(config->degree-1)/2){
		return childFirstKey;
	}
	//不满足树的定义
	if (index>0){
		index--;
	}
	//相邻的两个孩子匀一匀可以满足B+树定义
	if((now->children[index]->size+now->children[index+1]->size)/2>=(config->degree-1)/2){

	}
	//相邻的两个孩子不满足B+树定义：合并两个节点
	if(next->children==NULL){
		mergeLeafBTreeNode(config, now, index);
	} else {
		mergeNonLeafBTreeNode(config, now, index);
	}
	return childFirstKey;
}

int32 delete (BTree *config, uint8 *key){
	//TODO to finish

}

// //二分查找测试
// #include <stdio.h>
// int main(int argc, char const *argv[])
// {
// 	BTree *config = makeBTree(5, 1, 1);
// 	for(int i=0; i<4; i++){
// 		config->root->keys[i] = (uint8 *)malloc(sizeof(uint8));
// 	}
// 	config->root->keys[0][0] = 5;
// 	config->root->keys[1][0] = 5;
// 	config->root->keys[2][0] = 7;
// 	config->root->keys[3][0] = 9;
// 	config->root->size = 4;

// 	uint8 key=8;
// 	printf("%d\n", binarySearch(config->root, &key, config->keyLen));
// 	for(uint8 key=0; key<=10; key++){
// 		printf("key=%d; index=%d\n",key, binarySearch(config->root, &key, config->keyLen));
// 	}

// 	return 0;
// }

// //搜索查找测试
// #include <stdio.h>
// int main(int argc, char const *argv[])
// {
// 	BTree *config = makeBTree(5, 1, 1);

// 	for(int i=0; i<4; i++){
// 		config->root->keys[i] = (uint8 *)malloc(sizeof(uint8));
// 		config->root->values[i] = (uint8 *)malloc(sizeof(uint8));
// 	}

// 	config->root->keys[0][0] = 1;
// 	config->root->keys[1][0] = 5;
// 	config->root->keys[2][0] = 7;
// 	config->root->keys[3][0] = 9;

// 	config->root->values[0][0] = 1;
// 	config->root->values[1][0] = 5;
// 	config->root->values[2][0] = 7;
// 	config->root->values[3][0] = 9;
// 	config->root->size = 4;

// 	uint8 key=1;
// 	uint8 *value;
// 	value = searchBTree(config, &key);
// 	for(uint8 key=0; key<=10; key++){
// 		value = searchBTree(config, &key);
// 		if(value==NULL){
// 			printf("key=%d; value=NULL\n", key);
// 		} else {
// 			printf("key=%d; value=%d\n", key, *value);
// 		}
// 	}

// 	return 0;
// }

// // 单字节插入测试
// #include <stdio.h>
// int main(int argc, char const *argv[])
// {
// 	BTree *config = makeBTree(3, 1, 1);
// 	// uint8 data[] = {3,4,5,1,2,6,7,8};
// 	uint8 data[] = {8,7,6,5,4,3,2,1,8,65,43,2,4,56,7,3,6,4,3,45};
// 	uint8 *key;
// 	uint8 *value;
// 	for(int i=0; i<sizeof(data); i++){
// 		key = malloc(sizeof(uint8) * config->keyLen);
// 		value = malloc(sizeof(uint8) * config->valueLen);
// 		*key = data[i];
// 		*value = data[i];
// 		insertBTree(config, key, value);
// 		value = searchBTree(config, key);
// 		if(value==NULL){
// 			printf("key=%d; value=NULL\n", *key);
// 		} else {
// 			printf("key=%d; value=%d\n", *key, *value);
// 		}
// 	}

// 	BTreeNode* p = config->sqt;
// 	while(p!=NULL){
// 		for(int i=0; i<p->size; i++){
// 			printf("%d\n", *p->keys[i]);
// 		}
// 		p = p->next;
// 	}

// 	return 0;
// }

//多字节插入查找测试（8086 小端模式下，异常正常）
#include <stdio.h>
int main(int argc, char const *argv[])
{
	BTree *config = makeBTree(3, 4, 4);
	// uint8 data[] = {3,4,5,1,2,6,7,8};
	uint32 data[] = {258,100,111,110,120,4324,12312,234, 7, 6, 5, 4, 3, 2, 1, 8, 65, 43, 2, 4, 56, 7, 3, 6, 4, 3, 45};
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
	return 0;
}
