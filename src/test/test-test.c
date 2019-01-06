/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-test.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "test.h"
#include "util.h"

void test(){
	assertnull((void*)1,"测试失败检查1");
	assertnull((void *)2, "测试失败检查2");
}

void test1(){
	assertint(2, 1 + 1,"成功");
	assertint(1,2,"测试失败检查1");
}

void test2(){
	assertint(2, 1 + 1,"成功");
}

uint64 arr[10] = {0, 1, 2, 3, 0};
void fun(){
	uint64 value = 267;
	insertToArray((void **)arr, 10, 4, (void *)value);
	value=10;
}

void test3()
{
	
	fun();
	for(int i=0; i<10; i++){
		printf("%lld\n", arr[i]);
	}
}

TESTFUNC funcs[] = {test, test1, test2, test3};

int main(int argc, char const *argv[])
{
	// assert(0, "1==1");
	// assertchar('a', 'b', "返回值应该是char a");
	// assertchar('a', 'b', "返回值应该是char a");
	// assertshort(1, 2, "返回值应该是int 1");
	// assertfloat(1.11, 2.11, "返回值应该是float 1.11");
	// assertbool(1, 0, "返回值应该是true");
	// assertstring("abc", "cba", "返回值应该是abc");
	// assertbytearray("abc", "cba", 4, "返回值应该是[97, 98, 99, 0]");
	// assertnull((void *)1213, "返回值应该是NULL");
	// launchTests(3, test, test1, test2);
	// printf("0x%016llx\n",-1L);
	// uint64 a;
	// sscanf("0xffffffffffffffff", "0x%llx", &a);
	// printf("0x%016llx\n",a);
	// launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


