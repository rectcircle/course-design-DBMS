/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: btree.h
 * @description: 定义B+数内存版的结构定义与相关函数的定义，该代码用于探索B+树的原理
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-09-25
 ******************************************************************************/

#ifndef __BTREE_H__
#define __BTREE_H__

#include "global.h"
#include "util.h"

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

/** B+树结构 */
typedef struct BTree
{
	/**
	 * B+树的度、扇出、阶：
	 * 节点最多有degree个孩子
	 * 节点最多有degree个key
	 */
	uint32 degree;
	/** 每个键的字节数 */
	uint32 keyLen;
	/** 每个值的字节数 */
	uint32 valueLen;
	/** 目前树的深度 */
	uint32 depth;
	/** 是否唯一，是否允许出现重复的键 */
	int8 isUnique;
	/** B+树的根节点 */
	struct BTreeNode *root;
	/** 最小的叶子节点，用于遍历 */
	struct BTreeNode *sqt;
} BTree;

/** B+树节点结构 */
typedef struct BTreeNode
{
	/** 该节点在磁盘的页面号 */
	uint64 pageId;
	/** 
	 * key的数组
	 * 数组的长度为：(BTree.degree+1)，多余的一个空间用于节点分裂使用
	 * 每个元素的长度为：(keyLen)字节
	 */
	uint8** keys;
	/** 
	 * 若是叶子节点该字段不为null
	 * 数组长度为：(BTree.degree+1)，多余的一个空间用于节点分裂使用
	 * 每个元素的长度为：(valueLen)字节
	 */
	uint8** values;
	//已经被使用的key的数目
	uint32 size;
	/** 
	 * 指向孩子的指针数组
	 * 数组的长度为：BTree.degree+1
	 * 为null表示为叶子节点
	 */
	struct BTreeNode **children;
	/** 指向相邻节点的指针 */
	struct BTreeNode *next;
} BTreeNode;


/*****************************************************************************
 * 公开API
 ******************************************************************************/

/**
 * 创建一个可用B+树节点
 * @param config BTree类型结构，获取当前BTree基本配置
 * @param pageId 当前节点在磁盘中的块号
 * @param isLeaf 创建的节点是否是叶子节点
 * @return {BTreeNode*} 一个B+树的节点指针
 */
BTreeNode* makeBTreeNode(BTree* config, uint64 pageId, int8 isLeaf);

/**
 * 释放一个B+树节点所占用的内存
 * @param config BTree类型结构，获取当前BTree基本配置
 * @param node 要释放内存的节点
 * @param isLeaf 创建的节点是否是叶子节点
 * @return {BTreeNode*} 一个B+树的节点指针
 */
void freeBTreeNode(BTree *config, BTreeNode* node, int8 isLeaf);

/**
 * 创建一颗B+树，此函数还会创建一个根节点
 * @param degree B+树的度
 * @param pageId 当前节点在磁盘中的块号
 * @param keyLen key的字节数
 * @param valueLen value的字节数
 * @param isUnique 是否允许重复
 * @return {BTree*} 一颗可用的B+树
 */
BTree *makeBTree(uint32 degree, uint32 keyLen, uint32 valueLen, int8 isUnique);

/**
 * 从B+Tree中查找key对应的value，支持通过传出参数获取多个列表
 * @param config 合法的B+树
 * @param key 要查找的key
 * @param outNode 传出参数，不为NULL时，储存第一个value所在的BTreeNode
 * @param outIndex 传出参数，不为NULL时，储存第一个value所在的BTreeNode的下标
 * @return {uint8*} 第一个与key相等的value，一个字节数组指针，长度为config->valueLen
 */
uint8 *searchBTree(BTree *config, uint8 *key, BTreeNode **outNode, int32 *outIndex);

/**
 * 向BTree添加添加一条记录
 * 注意：不会进行重复判断，直接插入
 * 若想抱枕KV严格不重复（同一对KV在树中唯一），请先使用update，若返回0再进行插入
 * @param config 合法的B+树
 * @param key 要插入的key
 * @param value 要插入的value
 * @return {int32} -1 插入失败，1 表示插入成功
 */
int32 insertBTree(BTree *config, uint8 *key, uint8 *value);

/**
 * 从BTree中删除记录
 * @param config 合法的B+树
 * @param key 要删除的key
 * @param value 要删除的value可为NULL，为NULL表示删除所有满足key的记录
 * @return {int32} 表示删除的记录数目
 */
int32 removeBTree(BTree *config, uint8 *key, uint8 *value);

/**
 * 更新KV严格相等的valeu为newValue
 * @param config 合法的B+树
 * @param key 要更新的key
 * @param oldValue 要更新的之前的value
 * @param newValue 新的value
 * @return {int32} 0 没有选中的数据，1 更新成功
 */
int32 updateBTree(BTree *config, uint8 *key, uint8 *oldValue, uint8 *newValue);

#endif