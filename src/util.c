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
	if(len==0){
		*dest = NULL;
		return;
	}
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
	clearList(list);
	if(list!=NULL){
		free(list);
	}
}

void clearList(List *list){
	if(list==NULL){
		return;
	}
	ListNode *node = NULL;
	ListNode *tmp = list->head;
	while ((node=tmp)!=NULL){
		tmp = node->next;
		free(node);
	}
	list->tail = list->head = NULL;
	list->length = 0;
}

static void addNodeToList(List* list, ListNode* q){

	if(list->head ==NULL){
		list->head = q;
		list->tail = q;
	} else {
		list->tail->next = q;
		list->tail = q;
	}
	list->length++;
}

void addList(List *list, void *value){
	ListNode *q = (ListNode *)calloc(1, sizeof(ListNode));
	q->value = value;
	addNodeToList(list, q);
}

void addListToList(List *dest, List *src){
	if(dest==NULL||src==NULL){
		return;
	}
	if(dest->length==0){
		if(src->length!=0){
			dest->head = src->head;
			dest->tail = src->tail;
			dest->length = src->length;
		}
	} else {
		if(src->length!=0){
			dest->tail->next = src->head;
			dest->tail = src->tail;
			dest->length += src->length;
		}
	}
	src->head = src->tail = NULL;
	src->length = 0;
}

List* distinctList(List *dest, int (*compare)(void *a, void *b, void *args), void *args){
	if(dest==NULL || dest->length<=1){
		return makeList();
	}
	ListNode* targetTail = dest->head;
	ListNode* origin = dest->head->next;
	List* rubbish = makeList();
	while(origin!=NULL){
		ListNode *p = dest->head;
		int flag = 0;
		while (p!=origin){
			if(compare(p->value, origin->value, args)==0){
				//发现重复
				flag = 1;
				addNodeToList(rubbish, p);
				origin = origin->next;
				break;
			}
			p = p->next;
		}
		if(flag==0){
			//没有重复
			targetTail->next = origin;
			targetTail = origin;
			origin = origin->next;
		}
	}
	targetTail->next = NULL;
	dest->length -= rubbish->length;
	return rubbish;
}

void *removeHeadList(List *list){
	void * value;
	if(list->length==0){
		return NULL;
	}
	ListNode *node = list->head;
	value = node->value;
	list->head = node->next;
	if(list->head==NULL){
		list->tail = NULL;
	}
	list->length--;
	free(node);
	return value;
}

void foreachList(List *list, void (*func)(void *, void*), void* args){
	if(list==NULL || list->length==0){
		return;
	}
	ListNode* node = list->head;
	while(node!=NULL){
		func(node->value, args);
		node = node->next;
	}
}

/*****************************************************************************
 * 时间函数
 ******************************************************************************/

uint64 currentTimeMillis(){
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000ull + tv.tv_usec / 1000;
}