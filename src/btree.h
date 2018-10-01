#ifndef __BTREE_H__
#define __BTREE_H__

#include "global.h"


// B+树一些属性
typedef struct BTree
{
	//B+树的度、扇出、阶：
	//节点最多有degree个孩子
	//节点最多有degree-1个key
	uint32 degree;
	uint32 keyLen;   //每个键的字节数
	uint32 valueLen; //每个值的字节数
	uint32 depth;    //树的深度
	int32 isUnique;  //是否唯一，是否允许出现重复的键
	struct BTreeNode *root;  //B+树的根节点
	struct BTreeNode *sqt;   //最小的叶子节点，用于遍历
} BTree;
// B+树节点结构
typedef struct BTreeNode
{
	//该节点在磁盘的页面号
	uint64 pageId;
	//key的数组
	//数组的长度为：(BTree.degree-1)
	//每个元素的长度为：(keyLen)
	uint8** keys;
	//若是叶子节点该字段不为null
	//数组长度为：(BTree.degree-1)
	//每个元素的长度为：(valueLen)
	uint8** values;
	//已经被使用的key的数目
	uint32 size;
	//指向孩子的指针数组
	//数组的长度为：BTree.degree
	//为null表示为叶子节点
	struct BTreeNode **children;
	//若是叶子节点则不为null，指向下一个相邻的叶子节点
	struct BTreeNode *next;
} BTreeNode;

//创建一个可用节点
BTreeNode* makeBTreeNode(BTree* config, uint64 pageId, int8 isLeaf);
//创建一个B+树的配置
BTree* makeBTree(uint32 degree, uint32 keyLen, uint32 valueLen);
//从BTree中查找一条记录
uint8* search(BTree* config, uint8* key);
//向BTree添加添加一条记录
int32 insert(BTree *config, uint8 *key, uint8* value);
//向BTree中删除一条记录
int32 delete(BTree *config, uint8 *key);

//简单的宏函数
#endif