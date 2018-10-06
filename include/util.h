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
 * 对二级及以上指针的操作
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
 * 
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


#endif