/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: btree.h
 * @description: 定义B+数内存版的实现，该代码用于探索B+树的原理
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-09-25
 ******************************************************************************/

#include "btree.h"
#include <malloc.h>
#include <string.h>

/*****************************************************************************
 * 通用私有辅助函数
 ******************************************************************************/

private int32 binarySearch(BTreeNode* root, uint8* key, uint32 keyLen){
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

private BTreeNode *makeBTreeNode(BTree *config, uint64 pageId, int8 isLeaf){
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

private void freeBTreeNode(BTree *config, BTreeNode* node, int8 isLeaf){
	free(node->keys);
	if(isLeaf){ //如果是叶子节点
		free(node->values);
	} else { //如果不是叶子节点
		free(node->children);
	}
	free(node);
}

/*****************************************************************************
 * makeBTree的实现
 ******************************************************************************/
BTree *makeBTree(uint32 degree, uint32 keyLen, uint32 valueLen, int8 isUnique){
	//度小于3将会退化为链表，不允许创建
	if(degree<3) return NULL;
	BTree *config = (BTree *)malloc(sizeof(BTree));
	config->depth = 1;
	config->degree = degree;
	config->keyLen = keyLen;
	config->valueLen = valueLen;
	config->isUnique = isUnique;
	BTreeNode* root = makeBTreeNode(config, 0, 1);
	config->root = root;
	config->sqt = root;
	return config;
}

/*****************************************************************************
 * searchBTree的实现
 ******************************************************************************/
uint8 *searchBTree(BTree *config, uint8 *key, BTreeNode** outNode, int32* outIndex){
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
		if(outNode!=NULL && outIndex!=NULL){
			*outNode = root;
			*outIndex = idx;
		}
		return root->values[idx];
	}
	return NULL;
}

/*****************************************************************************
 * insertBTree的实现
 ******************************************************************************/

/**
 * 将现有节点分裂成两个节点，返回新创建的节点，平均分配
 */
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

/** 
 * 递归进行插入及树重建
 */
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

int32 insertBTree(BTree *config, uint8 *key, uint8 *value){
	//违反唯一约束
	if(config->isUnique && searchBTree(config, key, NULL, NULL)!=NULL){ 
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
	return 1;
}

/*****************************************************************************
 * removeBTree的实现
 ******************************************************************************/

/**
 * 根据现有节点和index、index+1号孩子进行合并，使孩子节点满足B+树的性质
 */
static void mergeBTreeNode(BTree *config, BTreeNode *nowNode, int32 index, int8 isLeaf){
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

/**
 * 均衡一下index和index+1号孩子的节点数，使树满足B+数的性质
 */
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

/**
 * 递归删除一个节点
 * @return 0 表示没有数据被选中删除； 其他表示被删除的数目
 */
static int32 removeFrom(BTree* config, BTreeNode* now, uint8 *key, uint8 *value, int32 level){
	int32 removeCnt=0;
	for(int32 index = binarySearch(now, key, config->keyLen); index<now->size; index++){
		//不存在该节点直接返回
		if (index == -1) return removeCnt;
		//是叶子节点
		if (level == config->depth){
			//非根节点的叶子节点不能删光了，要留一个
			//删空的话，回溯不好处理
			if(config->depth>1 && now->size<=1){
				return removeCnt;
			}
			//找不到该元素
			if(byteArrayCompare(config->keyLen, now->keys[index], key)!=0){
				return removeCnt;
			}
			//存在value只删除kv严格相等的数据
			if(value!=NULL && byteArrayCompare(config->valueLen, now->values[index], value)!=0){
				continue;
			}
			//释放数据和key占用的内存
			free(now->keys[index]);
			free(now->values[index]);
			deleteFromArray((void**)now->keys, now->size, index);
			deleteFromArray((void **)now->values, now->size, index);
			now->size--;
			removeCnt++;
			continue;
		}
		//非叶子节点
		if(byteArrayCompare(config->keyLen, key, now->keys[index])<0){
			// key < keys[index] 说明 key对应数据不在index这个孩子下，直接返回
			return removeCnt;
		}
		BTreeNode *next = now->children[index];
		removeCnt += removeFrom(config, next, key, value,level + 1);
		//更新当前节点指向next的key
		now->keys[index] = next->keys[0];

		//任然满足B+树的定义
		if (next->size>=(config->degree+1)/2){
			continue;
		}
		int32 idxBak = index;
		//不满足树的定义
		if (index>0){
			index--;
		}
		//相邻的两个孩子匀一匀可以满足B+树定义
		if((now->children[index]->size+now->children[index+1]->size)>=config->degree+1){
			balanceBTreeNode(config, now, index, level+1==config->depth);
			//说明根now->size没有发生变化，要恢复index
			index = idxBak;
			continue;
		}
		//相邻的两个孩子不满足B+树定义：合并两个节点
		mergeBTreeNode(config, now, index, level+1==config->depth);
		//当前节点已经不满足树的定义了，不能再删了，直接返回
		if(now->size<(config->degree+1)/2){
			return removeCnt;
		}
		//此时now->size发生了变化所有index要先-1，以抵消for中++的影响
		index = idxBak-1;
	}
	return removeCnt;
}

int32 removeBTree(BTree *config, uint8 *key, uint8 *value){
	int32 removeCnt = 0;
	for(;;){
		int32 cnt = removeFrom(config, config->root, key, value, 1);
		removeCnt += cnt;
		if(cnt==0){
			return removeCnt;
		}
		if(config->root->size==1 && config->depth>1){
			BTreeNode* tmp = config->root;
			config->root = config->root->children[0];
			freeBTreeNode(config, tmp, 0);
			config->depth--;
		}
	}
}

/*****************************************************************************
 * updateBTree的实现
 ******************************************************************************/
int32 updateBTree(BTree *config, uint8 *key, uint8 *oldValue, uint8 *newValue){
	BTreeNode *outNode;
	int32 outIndex;
	uint8 *value;
	value = searchBTree(config, key, &outNode, &outIndex);
	if(value==NULL){
		return 0;
	}
	int i = outIndex;
	while (outNode!=NULL){
		for(; i<outNode->size; i++){
			if(byteArrayCompare(config->keyLen,outNode->keys[i],key)!=0){
				return 0;
			}
			if(byteArrayCompare(config->valueLen, outNode->values[i], oldValue)==0){
				free(outNode->values[i]);
				outNode->values[i] = newValue;
				return 1;
			}
		}
		i = 0;
		outNode = outNode->next;
	}
	return 0;
}