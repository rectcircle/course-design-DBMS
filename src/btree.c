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
	node->keys = (uint8 **)malloc(sizeof(uint8*) * (config->degree));
	//初始化值和孩子节点
	if(isLeaf){ //如果是叶子节点
		node->values = (uint8 **)malloc(sizeof(uint8*) * (config->degree));
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
//例如 1 5 7 9
//划分的区间为(-无穷, 1), [1,5), [5, 7), [7, 9), [9,+无穷)
//返回区间的索引，从0开始
static int32 binarySearch(BTreeNode* root, uint8* key, uint32 keyLen){
	if(root->size==0){
		return 0;
	}
	int32 left = 0, right = root->size-1, mid;
	int32 result;
	while(left<right){
		mid = (left+right)>>1;
		result = byteArrayCompare(keyLen, root->keys[mid], key);
		if(result==0){ //keys[mid]==key
			left=mid;
			break;
		} else if(result<0){ //keys[mid]<key
			left=mid+1;
		} else { //keys[mid]>key
			right=mid-1;
		}
	}
	result = byteArrayCompare(keyLen, root->keys[left], key);
	if (result>0){
		return left;
	}
	return left+1;
}

//从BTree中查找一条记录
uint8 *search(BTree *config, uint8 *key){
	BTreeNode* root = config->root;
	//TODO 修改成使用使用层级判断的方式
	while(root->children!=NULL){ //一直查找到叶子节点
		root = root->children[binarySearch(root, key, config->keyLen)];
	}
	int32 idx = binarySearch(root, key, config->keyLen)-1;
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

}

//根据现有叶子节点分裂成两个节点，返回新创建的节点
static BTreeNode *splitLeafBTreeNode(BTree *config, BTreeNode *nowNode, int8 isLeaf){
	BTreeNode* newNode = makeBTreeNode(config, 0, isLeaf);
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
static BTreeNode *splitNonLeafBTreeNode(BTree *config, BTreeNode *nowNode, int8 isLeaf){
	BTreeNode* newNode = makeBTreeNode(config, 0, isLeaf);
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

//递归进行插入及树重建
static BTreeNode* insertTo(BTree *config, BTreeNode* now, uint8 *key, uint8 *value){
	//是叶子节点
	if(now->children==NULL){
		int32 index = binarySearch(now, key, config->keyLen);
		insertToArray((void **)now->keys, config->degree, index, (void *)key);
		insertToArray((void **)now->values, config->degree, index, (void *)value);
		now->size++;
		if(now->size<config->degree){ //未满
			return NULL;
		}
		//已满
		BTreeNode *newNode = splitLeafBTreeNode(config, now, 1);
		return newNode;
	} 
	//不是叶子节点
	BTreeNode *next = now->children[binarySearch(now, key, config->keyLen)];
	BTreeNode *result = insertTo(config, next, key, value);

	//非叶子节点后续处理
	if(result==NULL){
		return NULL;
	}
	uint8 *childKey;
	if (result->children==NULL){
		//result是叶子节点
		childKey = result->keys[0];
	} else {
		//result是非叶子节点，分割的key备份在最后
		childKey = result->keys[result->size];
	}
	int32 index = binarySearch(now, childKey, config->keyLen);
	insertToArray((void **)now->keys, config->degree, index, (void *)childKey);
	insertToArray((void **)now->children, config->degree + 1, index + 1, (void *)result);

	now->size++;
	if(now->size<config->degree){ //未满
		return NULL;
	}
	//已满
	BTreeNode *newNode = splitNonLeafBTreeNode(config, now, 0);
	return newNode;
}

//向BTree添加添加一条记录
//返回0 插入成功，返回其他表示插入失败
int32 insert(BTree *config, uint8 *key, uint8 *value){
	//违反唯一约束
	if(config->isUnique && search(config, key)!=NULL){ 
		return -1;
	}

	BTreeNode *newChild = insertTo(config, config->root, key, value);
	if(newChild!=NULL){
		BTreeNode *newRoot = makeBTreeNode(config, 0, 0);
		newRoot->keys[0] = newChild->keys[0];
		newRoot->children[0] = config->root;
		newRoot->children[1] = newChild;
		newRoot->size = 1;
		config->root = newRoot;
	}
	return 0;
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
// 	config->root->keys[0][0] = 1;
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
// 	value = search(config, &key);
// 	for(uint8 key=0; key<=10; key++){
// 		value = search(config, &key);
// 		if(value==NULL){
// 			printf("key=%d; value=NULL\n", key);
// 		} else {
// 			printf("key=%d; value=%d\n", key, *value);
// 		}
// 	}

// 	return 0;
// }

// 插入测试
#include <stdio.h>
int main(int argc, char const *argv[])
{
	BTree *config = makeBTree(3, 1, 1);
	// uint8 data[] = {3,4,5,1,2,6,7,8};
	uint8 data[] = {8,7,6,5,4,3,2,1,8,65,43,2,4,56,7,3,6,4,3,45};
	uint8 *key;
	uint8 *value;
	for(int i=0; i<sizeof(data); i++){
		key = malloc(sizeof(uint8) * config->keyLen);
		value = malloc(sizeof(uint8) * config->valueLen);
		*key = data[i];
		*value = data[i];
		insert(config, key, value);
		value = search(config, key);
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

	return 0;
}