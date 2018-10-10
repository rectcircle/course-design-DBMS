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
#include <stdarg.h>
#include <setjmp.h>

/**
 * 可以传给launchTests的类型
 */
typedef void (*TESTFUNC)(void);

/******************************************************************************
 * 运行测试程序
 * @param len 可变参数列表长度
 * @param {...} TESTFUNC 类型的可变参数
 * @return {int} 测试失败的数目
 ******************************************************************************/
int launchTests(int len, ...);

/** 运行测试程序：参数为数组 */
int launchTestArray(int len, TESTFUNC* funcArray);

/******************************************************************************
 * 常用的断言函数
 * 直接调用：若测试失败（即expect!=actual），直接通过exit(1)退出
 * 使用launchTestxxx：将使用长跳机制执行跳出
 * @param expect 期望结果
 * @param actual 程序运行结果
 * @param errMsg 测试说明
 ******************************************************************************/
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