// /**
//  * Copyright (c) 2018, rectcircle. All rights reserved.
//  *
//  * @file test-test.cpp 
//  * @author rectcircle 
//  * @date 2018-10-06 
//  * @version 0.0.1
//  */
// #include "test.h"
// #include "util.h"

// void test(){
// 	assertnull((void*)1,"测试失败检查1");
// 	assertnull((void *)2, "测试失败检查2");
// }

// void test1(){
// 	assertint(2, 1 + 1,"成功");
// 	assertint(1,2,"测试失败检查1");
// }

// void test2(){
// 	assertint(2, 1 + 1,"成功");
// }

// uint64 arr[10] = {0, 1, 2, 3, 0};
// void fun(){
// 	uint64 value = 4;
// 	insertToArray((void **)arr, 10, 4, (void *)value);
// }

// void test3()
// {
	
// 	fun();
// 	for(int i=0; i<10; i++){
// 		printf("%lld\n", arr[i]);
// 	}
// }

// TESTFUNC funcs[] = {test, test1, test2, test3};

// int main(int argc, char const *argv[])
// {
// 	// assert(0, "1==1");
// 	// assertchar('a', 'b', "返回值应该是char a");
// 	// assertchar('a', 'b', "返回值应该是char a");
// 	// assertshort(1, 2, "返回值应该是int 1");
// 	// assertfloat(1.11, 2.11, "返回值应该是float 1.11");
// 	// assertbool(1, 0, "返回值应该是true");
// 	// assertstring("abc", "cba", "返回值应该是abc");
// 	// assertbytearray("abc", "cba", 4, "返回值应该是[97, 98, 99, 0]");
// 	// assertnull((void *)1213, "返回值应该是NULL");
// 	// launchTests(3, test, test1, test2);
// 	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
// 	return 0;
// }

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
pthread_mutex_t g_mutex;
void test_fun(void);
static void thread_init(void)
{
	//初始化锁的属性
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE_NP); //设置锁的属性为可递归

	//设置锁的属性
	pthread_mutex_init(&g_mutex, &attr);

	//销毁
	pthread_mutexattr_destroy(&attr);
}
//线程执行函数
void *thr_fun(void *arg)
{
	int ret;
	ret = pthread_mutex_lock(&g_mutex);
	if (ret != 0)
	{
		perror("thread  pthread_mutex_lock");
		exit(1);
	}
	printf("this is a thread !/n");
	test_fun();
	ret = pthread_mutex_unlock(&g_mutex);
	if (ret != 0)
	{
		perror("thread  pthread_mutex_unlock");
		exit(1);
	}
	return NULL;
}
//测试函数
void test_fun(void)
{
	int ret;
	ret = pthread_mutex_lock(&g_mutex);
	if (ret != 0)
	{
		perror("test pthread_mutex_lock");
		exit(1);
	}
	printf("this is a test!/n");
	ret = pthread_mutex_unlock(&g_mutex);
	if (ret != 0)
	{
		perror("test pthread_mutex_unlock");
		exit(1);
	}
}

int main(int argc, char *argv[])
{
	int ret;
	thread_init();

	pthread_t tid;
	ret = pthread_create(&tid, NULL, thr_fun, NULL);
	if (ret != 0)
	{
		perror("thread  create");
		exit(1);
	}
	pthread_join(tid, NULL);
	return 0;
}
