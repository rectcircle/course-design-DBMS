/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: global.h
 * @description: 定义平台无关的基本数据类型和通用类型
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-09-25
 ******************************************************************************/
#pragma once
#ifndef __GLOBAL_H__
#define __GLOBAL_H__

#define MAX_UINT32 0xffffffffu;

typedef unsigned int uint32;
typedef unsigned long long uint64;
typedef unsigned char uint8;
typedef unsigned short uint16;

typedef char int8;
typedef int int32;
typedef short int16;
typedef long long int64;

/** 关系运算符 */
/** 等于 */
#define RELOP_EQ 0
/** 不等于 */
#define RELOP_NEQ 1
/** 小于 */
#define RELOP_LT 2
/** 小于等于 */
#define RELOP_LTE 3
/** 大于 */
#define RELOP_GT 4
/** 大于等于 */
#define RELOP_GTE 5

#ifdef PROFILE_TEST
#define private extern
#else
#define private static
#endif

#endif
