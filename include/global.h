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
#ifdef PROFILE_TEST
#define private extern
#else
#define private static
#endif

#endif
