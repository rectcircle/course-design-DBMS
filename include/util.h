/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: btree.h
 * @description: 通用工具的声明
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-03
 ******************************************************************************/
#pragma once
#ifndef __UTIL_H__
#define __UTIL_H__

#include "global.h"
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>

/*****************************************************************************
 * 结构定义
 ******************************************************************************/

typedef struct Array
{
	void *array;
	int32 length;
} Array;

typedef struct ListNode
{
	void *value;
	struct ListNode *next;
} ListNode;
typedef struct List
{
	struct ListNode *head;
	struct ListNode *tail;
	int32 length;
} List;

/*****************************************************************************
 * 数组操作
 ******************************************************************************/

/**
 * 字节数组的比较大小
 * @param len 待比较两个字节数组的长度
 * @param a   待比较的数组a
 * @param b   待比较的数组b
 * @return a<b 返回 小于0的数
 *         a=b 返回 0
 *         b>b 返回 大于0的数
 */
int32 byteArrayCompare(uint32 len, uint8 *a, uint8 *b);

/**
 * 批量拷贝：
 * 对二级及指针的操作为浅拷贝
 * 对uint64数组操作为深拷贝
 * 将srcArray的前srcLen个元素插入到destArray的index及之后
 * 最后destArray数组的最后srcLen个元素将会被抛弃
 * 
 * @param destArray 目标数组，拷贝到的位置
 * @param distLen   目标数组的最大长度，用于防止越界
 * @param index     插入的第一个元素所在的位置
 * @param srcArray  源数组，拷贝数据的提供者
 * @param srcLen    要拷贝的长度
 * @return -1 不能拷贝，存放不下, 0正常
 */
int32 batchInsertToArray(
	void** destArray, uint32 distLen, 
	uint32 index, 
	void** srcArray, uint32 srcLen);

/**
 * 对二级及以上指针的操作，向数组array的index位置插入值value
 * 对二级及指针的操作为浅拷贝
 * 对uint64数组操作为深拷贝
 * @param array 目标数组，拷贝到的位置
 * @param len   目标数组的最大长度，用于防止越界
 * @param index 插入的第一个元素所在的位置
 * @param value 要拷贝的元素
 * @return -1 不能拷贝，存放不下； 0 正常
 */
int32 insertToArray(void** array, uint32 len, uint32 index, void* value);

/**
 * 批量删除
 * 对二级及以上指针的操作
 * 从数组array删除第index位置开始的delLen个元素
 * 
 * @param array 目标数组，拷贝到的位置
 * @param len   目标数组的长度，此处为有效数据的长度
 * @param index 删除的第一个元素所在的位置
 * @param delLen 要删除的元素数目
 * @return -1 index异常； 0 正常
 */
int32 batchDeleteFromArray(
	void** array, uint32 len,
	uint32 index, uint32 delLen);

/**
 * 对二级及以上指针的操作，从数组array删除第index位置的元素
 * 
 * @param array 目标数组，拷贝到的位置
 * @param len   目标数组的长度，此处为有效数据的长度
 * @param index 删除的第一个元素所在的位置
 * @return -1 index异常； 0 正常
 */
int32 deleteFromArray(void** array, uint32 len, uint32 index);

/**
 * 申请内存并拷贝数组
 * @param dest 目的
 * @param src 源
 * @param len 长度
 */
void newAndCopyByteArray(uint8 **dest, uint8 *src, uint32 len);

/*****************************************************************************
 * 简单容器使用
 ******************************************************************************/

/**
 * 创建一个List
 * @return {List*} 一个可用链表
 */
List* makeList();

/**
 * 释放一个List的内存
 */
void freeList(List* list);

/**
 * 清空一个List
 */
void clearList(List* list);

/**
 * 向链表末位插入一个元素
 * @param list 一个链表
 * @param value 值
 */
void addList(List* list, void* value);

/**
 * 向链表末位插入一个链表，并清空原有链表
 * @param dest 被插入的链表
 * @param src 数据提供者，会清空
 */
void addListToList(List* dest, List* src);

/**
 * 链表去重
 * 
 * @param dest 被插入的链表
 * @param dest int (*compare)(void* a, void *b, void* args) 比较函数 返回0表示相同
 * @param args 闭包参数
 * @return 被剔除的元素列表, 需要用户进行内存释放
 */
List* distinctList(List* dest, int (*compare)(void* a, void *b, void* args), void*args);

/**
 * 删除链表的第一个元素
 * @param list 一个链表
 * @return 返回value值
 */
void *removeHeadList(List *list);

/**
 * 高阶函数foreach，遍历list中的每一个元素
 * @param list 待操作List
 * @param func 操作函数第一个参数为list中的value, 第二个参数为args
 */
void foreachList(List *list, void (*func)(void *, void *), void *args);

/*****************************************************************************
 * 时间函数
 ******************************************************************************/

/**
 * 获取当前时刻时间戳（毫秒）
 * @return 时间戳
 */
uint64 currentTimeMillis();

/*****************************************************************************
 * 宏函数
 ******************************************************************************/

/**
 * 64位整形字节序转换
 */
#define __swap64(val) (((val) >> 56) |                        \
					 (((val)&0x00ff000000000000ll) >> 40) | \
					 (((val)&0x0000ff0000000000ll) >> 24) | \
					 (((val)&0x000000ff00000000ll) >> 8) |  \
					 (((val)&0x00000000ff000000ll) << 8) |  \
					 (((val)&0x0000000000ff0000ll) << 24) | \
					 (((val)&0x000000000000ff00ll) << 40) | \
					 (((val) << 56)))

#define htonll(val) ((htons(1)==1) ? val : __swap64(val))
#define ntohll(val) htonll(val)

#endif