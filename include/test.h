/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: test.h
 * @description: 简单测试框架，函数声明
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-06
 ******************************************************************************/
#pragma once
#ifndef __TEST_H__
#define __TEST_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * 测试断言，cond为0时，测试不通过输出errMsg并以-1退出码退出
 * @param cond 条件 
 * @param errMsg 错误信息
 * @exit -1 测试不通过
 */
void assert(int cond, const char *errMsg);

/**
 * 针对基本数据类型的断言
 */
void assertchar(char expect, char actual, const char *errMsg);
void assertshort(short expect, short actual, const char *errMsg);
void assertint(int expect, int actual, const char *errMsg);
void assertlong(long expect, long actual, const char *errMsg);
void assertlonglong(long long expect, long long actual, const char *errMsg);

void assertuchar(unsigned char expect, unsigned char actual, const char *errMsg);
void assertushort(unsigned short expect, unsigned short actual, const char *errMsg);
void assertuint(unsigned int expect, unsigned int actual, const char *errMsg);
void assertulong(unsigned long expect, unsigned long actual, const char *errMsg);
void assertulonglong(unsigned long long expect, unsigned long long actual, const char *errMsg);

void assertfloat(float expect, float actual, const char *errMsg);
void assertdouble(double expect, double actual, const char *errMsg);
void assertlongdouble(long double expect, long double actual, const char *errMsg);

void assertbool(int expect, int actual, const char *errMsg);

void assertstring(const char *expect, const char *actual, const char *errMsg);

void assertbytearray(const char* expect, const char* actual, int len, const char *errMsg);

void assertnull(void *actual, const char *errMsg);

#endif