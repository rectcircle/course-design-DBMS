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


void freeBTreeNode(BTree *config, BTreeNode* node, int8 isLeaf){
	free(node->keys);
	if(isLeaf){ //如果是叶子节点
		free(node->values);
	} else { //如果不是叶子节点
		free(node->children);
	}
	free(node);
}


//创建一个B+树，并创建根节点
BTree *makeBTree(uint32 degree, uint32 keyLen, uint32 valueLen){
	//度小于3将会退化为链表，不允许创建
	if(degree<3) return NULL;
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
//input   1  2 5 6 7 8 9 10
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

//从将数组src中的前srcLen个元素插入到dest的index位置
static int32 batchInsertToArray(void** destArray, uint32 distLen, uint32 index, void** srcArray, uint32 srcLen){
	if(index<0 || index+srcLen-1>=distLen){
		return -1;
	}
	//搬移目标数组元素
	for(int32 i=distLen-1; i>=index+srcLen; i--){
		destArray[i] = destArray[i - srcLen];
	}
	//拷贝
	memcpy(destArray+index, srcArray, sizeof(void *)* srcLen);
	return 0;
}

//在数组array的第index位置插入一个元素
//0表示成功；-1表示无法插入
static int32 insertToArray(void** array, uint32 len, uint32 index, void* value){
	// if(index<0 || index>=len){
	// 	return -1;
	// }
	// for(int32 i=len-1; i>index; i--){
	// 	array[i]=array[i-1];
	// }
	// array[index] = value;
	// return 0;
	return batchInsertToArray(array,len,index,&value,1);
}

//从数组array删除第index位置开始的delLen个元素
//0表示成功；-1表示无法删除
static int32 batchDeleteFromArray(void** array, uint32 len, uint32 index, uint32 delLen){
	if(index<0 || index>=len){
		return -1;
	}
	for(int32 i=index; i<len-delLen; i++){
		array[i]=array[i+delLen];
	}
	return 0;
}

//从数组array删除第index位置的元素
//0表示成功；-1表示无法删除
static int32 deleteFromArray(void** array, uint32 len, uint32 index){
	return batchDeleteFromArray(array, len, index, 1);
}

//将现有节点分裂成两个节点，返回新创建的节点
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

//根据现有节点和index、index+1号孩子进行合并，使孩子节点满足B+树的性质
static void *mergeBTreeNode(BTree *config, BTreeNode *nowNode, int32 index, int8 isLeaf){
	BTreeNode *idxNode = nowNode->children[index];
	BTreeNode *idx1Node = nowNode->children[index + 1];
	uint32 idxLen = idxNode->size;
	uint32 idx1Len = idx1Node->size;
	//从idx1拷贝到idx
	batchInsertToArray((void**)idxNode->keys, config->degree+1, idxLen, (void**)idx1Node->keys, idx1Len);
	if(isLeaf)
		batchInsertToArray((void**)idxNode->values, config->degree+1, idxLen, (void**)idx1Node->values, idx1Len);
	else 
		batchInsertToArray((void**)idxNode->children, config->degree+1, idxLen, (void**)idx1Node->children, idx1Len);
	idxNode->size = idxLen+idx1Len;
	idxNode->next = idx1Node->next;
	//删除父亲中关于idx1的记录
	deleteFromArray((void**)nowNode->keys, config->degree+1, index+1);
	deleteFromArray((void**)nowNode->children, config->degree+1, index+1);
	nowNode->size--;
	//释放idx1的内存
	freeBTreeNode(config,idx1Node,isLeaf);
}

//均衡一下index和index+1号孩子的节点数，使树满足B+数的性质
static void balanceBTreeNode(BTree *config, BTreeNode *nowNode, int32 index, int8 isLeaf){
	BTreeNode* idxNode = nowNode->children[index];
	BTreeNode *idx1Node = nowNode->children[index+1];
	uint32 idxLen = idxNode->size;
	uint32 idx1Len = idx1Node->size;
	uint32 len = (idxLen + idx1Len) / 2;
	uint32 moveLen;
	if(idxLen < idx1Len){ //从idx+1向idx迁移
		moveLen = idx1Len - len;
		//搬移key
		batchInsertToArray((void**)idxNode->keys, config->degree+1, idxLen, (void**)idx1Node->keys, moveLen);
		//搬移数据
		if(isLeaf) 
			batchInsertToArray((void**)idxNode->values, config->degree+1, idxLen, (void**)idx1Node->values, moveLen);
		else 
			batchInsertToArray((void**)idxNode->children, config->degree+1, idxLen, (void**)idx1Node->children, moveLen);
		//删除
		batchDeleteFromArray((void**)idx1Node->keys, idx1Len, 0, moveLen);
		if(isLeaf) batchDeleteFromArray((void**)idx1Node->values, idx1Len, 0, moveLen);
		else batchDeleteFromArray((void**)idx1Node->children, idx1Len, 0, moveLen);
		//修改计数
		idxNode->size += moveLen;
		idx1Node->size -= moveLen;
	} else { //从idx向idx+1迁移
		moveLen = idxLen - len;
		//搬移key
		batchInsertToArray((void**)idx1Node->keys, config->degree+1, 0, (void**)(idxNode->keys+idxLen-moveLen),  moveLen);
		//搬移数据
		if(isLeaf) 
			batchInsertToArray((void**)idx1Node->values, config->degree+1, 0, (void**)(idxNode->values+idxLen-moveLen),  moveLen);
		else 
			batchInsertToArray((void**)idx1Node->children, config->degree+1, 0, (void**)(idxNode->children+idxLen-moveLen),  moveLen);
		//修改计数
		idxNode->size -= moveLen;
		idx1Node->size += moveLen;
	}
	//修改父亲的key
	nowNode->keys[index + 1] = idx1Node->keys[0];
}

static void removeFrom(BTree* config, BTreeNode* now, uint8 *key, int32 level){
	int32 index = binarySearch(now, key, config->keyLen);
	//是叶子节点
	if (level == config->depth){
		//找不到该元素
		if(index==-1||byteArrayCompare(config->keyLen, now->keys[index], key)!=0){
			return;
		}
		//释放数据和key占用的内存
		free(now->keys[index]);
		free(now->values[index]);
		deleteFromArray((void**)now->keys, now->size, index);
		deleteFromArray((void **)now->values, now->size, index);
		now->size--;
		return;
	}
	//非叶子节点
	if(index==-1) return; //不存在该节点直接返回（因为小于最小值）

	BTreeNode *next = now->children[index];
	removeFrom(config, next, key, level+1);
	//更新当前节点指向next的key
	now->keys[index] = next->keys[0];

	//任然满足B+树的定义
	if (next->size>=(config->degree+1)/2){
		return;
	}
	//不满足树的定义
	if (index>0){
		index--;
	}
	//相邻的两个孩子匀一匀可以满足B+树定义
	if((now->children[index]->size+now->children[index+1]->size)>=config->degree+1){
		balanceBTreeNode(config, now, index, level+1==config->depth);
		return;
	}
	//相邻的两个孩子不满足B+树定义：合并两个节点
	mergeBTreeNode(config, now, index, level+1==config->depth);
	return;
}

int32 removeBTree(BTree *config, uint8 *key){
	//TODO to finish
	removeFrom(config, config->root, key, 1);
	if(config->root->size==1 && config->depth>1){
		BTreeNode* tmp = config->root;
		config->root = config->root->children[0];
		freeBTreeNode(config, tmp, 0);
		config->depth--;
	}
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