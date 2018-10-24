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
#include "redolog.h"

void test(){
	printf("测试编译\n");
}

TESTFUNC funcs[] = {
	test,

};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


