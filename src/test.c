/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: test.c
 * @description: 简单测试框架
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-06
 ******************************************************************************/
#include "test.h"


jmp_buf* TESTENV = NULL;

int launchTests(int len, ...){
	TESTENV = (jmp_buf *)malloc(sizeof(jmp_buf));
	va_list valist;
	va_start(valist, len);
	int sum=0;
	for(int i=0; i<len; i++){
		int ans = setjmp(*TESTENV);
		if(ans==0){
			va_arg(valist, TESTFUNC)();
		} else if(ans==1) {
			sum += 1;
		}
		
	}
	va_end(valist);
	printf("\e[1;33m测试结果：\e[0m\e[1m总共 %d \e[1;32m成功 %d \e[1;31m失败 %d\e[0m\n", len, len - sum, sum);
	free(TESTENV);
	TESTENV = NULL;
	return sum;
}

int launchTestArray(int len, TESTFUNC* funcArray){
	TESTENV = (jmp_buf *)malloc(sizeof(jmp_buf));
	int sum=0;
	for(int i=0; i<len; i++){
		int ans = setjmp(*TESTENV);
		if(ans==0){
			funcArray[i]();
		} else if(ans==1) {
			sum += 1;
		}
		
	}
	printf("\e[1;33m测试结果：\e[0m\e[1m总共 %d \e[1;32m成功 %d \e[1;31m失败 %d\e[0m\n", len, len - sum, sum);
	free(TESTENV);
	TESTENV = NULL;
	return sum;
}


/*****************************************************************************
 * assert
 ******************************************************************************/
static int assert(int cond, const char * errMsg){
	if(!cond){
		fprintf(stderr, "\e[1;31m测试失败：\e[1;33m%s\e[0m\n", errMsg);
		return 1;
	}
	return 0;
}

static void exitTest(int result){
	if(result==1){
		if (TESTENV==NULL){
			exit(1);
		} else {
			longjmp(*TESTENV, 1);
		}
	}
	
}

/*****************************************************************************
 * assert
 ******************************************************************************/
void assertchar(char expect, char actual, const char *errMsg){
	char* buffer = malloc(sizeof(char)*strlen(errMsg)+128);
	sprintf(buffer, "\n\e[0m期望：\e[1;32m\n\t%c\n\e[0m实际：\e[1;31m\n\t%c\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m", expect, actual, errMsg);
	int result = assert(expect==actual, buffer);
	free(buffer);
	exitTest(result);
}
void assertshort(short expect, short actual, const char *errMsg){
	assertlonglong(expect, actual, errMsg);
}
void assertint(int expect, int actual, const char *errMsg){
	assertlonglong(expect, actual, errMsg);
}
void assertlong(long expect, long actual, const char *errMsg){
	assertlonglong(expect, actual, errMsg);
}
void assertlonglong(long long expect, long long actual, const char *errMsg){
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 128);
	sprintf(buffer, "\n\e[0m期望：\e[1;32m\n\t%lld\n\e[0m实际：\e[1;31m\n\t%lld\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m", expect, actual, errMsg);
	int result = assert(expect == actual, buffer);
	free(buffer);
	exitTest(result);
}

void assertuchar(unsigned char expect, unsigned char actual, const char *errMsg){
	assertulonglong(expect, actual, errMsg);
}
void assertushort(unsigned short expect, unsigned short actual, const char *errMsg){
	assertulonglong(expect, actual, errMsg);
}
void assertuint(unsigned int expect, unsigned int actual, const char *errMsg){
	assertulonglong(expect, actual, errMsg);
}
void assertulong(unsigned long expect, unsigned long actual, const char *errMsg){
	assertulonglong(expect, actual, errMsg);
}
void assertulonglong(unsigned long long expect, unsigned long long actual, const char *errMsg){
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 128);
	sprintf(buffer, "\n\e[0m期望：\e[1;32m\n\t%llu\n\e[0m实际：\e[1;31m\n\t%llu\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m", expect, actual, errMsg);
	int result = assert(expect == actual, buffer);
	free(buffer);
	exitTest(result);
}

void assertfloat(float expect, float actual, const char *errMsg){
	assertlongdouble(expect, actual, errMsg);
}
void assertdouble(double expect, double actual, const char *errMsg){
	assertlongdouble(expect, actual, errMsg);
}
void assertlongdouble(long double expect, long double actual, const char *errMsg){
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 256);
	sprintf(buffer, "\n\e[0m期望：\e[1;32m\n\t%Lf\n\e[0m实际：\e[1;31m\n\t%Lf\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m", expect, actual, errMsg);
	int result = assert(expect == actual, buffer);
	free(buffer);
	exitTest(result);
}

void assertbool(int expect, int actual, const char *errMsg){
	static char boolStringArray[2][6] = {"false", "true"};
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 256);
	sprintf(
		buffer, 
		"\n\e[0m期望：\e[1;32m\n\t%s\n\e[0m实际：\e[1;31m\n\t%s\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m", 
		boolStringArray[expect?1:0], 
		boolStringArray[actual?1:0], 
		errMsg);
	int result = assert(expect == actual, buffer);
	free(buffer);
	exitTest(result);
}

void assertstring(const char *expect, const char *actual, const char *errMsg){
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 256 + strlen(expect)+strlen(actual)+2);
	sprintf(
		buffer,
		"\n\e[0m期望：\e[1;32m\n\t%s\n\e[0m实际：\e[1;31m\n\t%s\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m",
		expect,
		actual,
		errMsg);
	int result = assert(strcmp(expect, actual) == 0, buffer);
	free(buffer);
	exitTest(result);
}

static char* byteArrayToString(const char * array, int len){
	char *str = malloc(len*5+3);
	str[0] = '[';
	str[1] = '\0';
	if(len>0){
		char num[4];
		sprintf(num, "%d", array[0]);
		strcat(str,num);
		for(int i=1; i<len; i++){
			sprintf(num, ", %d", array[i]);
			strcat(str, num);
		}
	}
	strcat(str, "]");
	return str;
}

void assertbytearray(const char *expect, const char *actual, int len, const char *errMsg){
	char *expectStr = byteArrayToString(expect, len);
	char *actualStr = byteArrayToString(actual, len);
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 256 + strlen(expectStr) + strlen(actualStr) + 2);
	sprintf(
		buffer,
		"\n\e[0m期望：\e[1;32m\n\t%s\n\e[0m实际：\e[1;31m\n\t%s\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m",
		expectStr,
		actualStr,
		errMsg);
	int result = assert(memcmp((void *)expect, (void *)actual, len) == 0, buffer);
	free(expectStr);
	free(actualStr);
	free(buffer);
	exitTest(result);
}

void assertnull(void *actual, const char *errMsg){
	char *buffer = malloc(sizeof(char) * strlen(errMsg) + 256);
	sprintf(
		buffer,
		"\n\e[0m期望：\e[1;32m\n\tNULL指针\n\e[0m实际：\e[1;31m\n\t%p\n\e[0m测试说明：\e[1;33m\n\t%s\e[0m",
		actual,
		errMsg);
	int result = assert(actual == NULL, buffer);
	free(buffer);
	exitTest(result);
}