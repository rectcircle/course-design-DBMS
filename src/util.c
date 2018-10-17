/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: btree.h
 * @description: 通用工具的实现
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-10-03
 ******************************************************************************/

#include "util.h"
#include <malloc.h>

/*****************************************************************************
 * byteArrayCompare
 ******************************************************************************/
int32 byteArrayCompare(uint32 len, uint8* a, uint8* b){
	for(uint32 i=0; i<len; i++){
		if(a[i]!=b[i]){
			return a[i]-b[i];
		}
	}
	return 0;
}

/*****************************************************************************
 * batchInsertToArray
 ******************************************************************************/
int32 batchInsertToArray(void** destArray, uint32 distLen, uint32 index, void** srcArray, uint32 srcLen){
	if(index<0 || index+srcLen-1>=distLen){
		return -1;
	}
	//搬移目标数组元素
	for(int32 i=distLen-1; i>=index+srcLen; i--){
		destArray[i] = destArray[i - srcLen];
	}
	//拷贝
	memcpy(destArray+index, srcArray, sizeof(void *)* srcLen);
	return 0;
}

/*****************************************************************************
 * insertToArray
 ******************************************************************************/
int32 insertToArray(void** array, uint32 len, uint32 index, void* value){
	return batchInsertToArray(array,len,index,&value,1);
}

/*****************************************************************************
 * batchDeleteFromArray
 ******************************************************************************/
int32 batchDeleteFromArray(void** array, uint32 len, uint32 index, uint32 delLen){
	if(index<0 || index>=len){
		return -1;
	}
	for(int32 i=index; i<len-delLen; i++){
		array[i]=array[i+delLen];
	}
	return 0;
}

/*****************************************************************************
 * deleteFromArray
 ******************************************************************************/
int32 deleteFromArray(void** array, uint32 len, uint32 index){
	return batchDeleteFromArray(array, len, index, 1);
}

/*****************************************************************************
 * newAndCopyByteArray
 ******************************************************************************/
void newAndCopyByteArray(uint8 **dest, uint8 *src, uint32 len){
	*dest = (uint8*) malloc(len);
	memcpy(*dest,src, len);
}

/*****************************************************************************
 * 链表操作
 ******************************************************************************/

List *makeList()
{
	return (List *)calloc(1,sizeof(List));
}

void freeList(List *list){
	ListNode *node = NULL;
	ListNode *tmp = list->head;
	while ((node=tmp)!=NULL){
		tmp = node->next;
		free(node);
	}
	free(list);
}

void addList(List *list, void *value){
	ListNode* q = (ListNode *)calloc(1, sizeof(ListNode));
	q->value = value;
	if(list->head ==NULL){
		list->head = q;
		list->tail = q;
	} else {
		list->tail->next = q;
		list->tail = q;
	}
	list->length++;
}