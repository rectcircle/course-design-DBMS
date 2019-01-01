/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-lrucache.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "simpledatabase.h"
#include "test.h"
#include <stdio.h>
#include <unistd.h>

//=========test All=========

void *printDatabaseItem(struct Entry *entry, void *args){
	char* databasename = calloc(1, entry->keyLen+1);
	memcpy(databasename, entry->key, entry->keyLen);
	printf("%s\t%d\n", databasename, ((HashMap*)entry->value)->size);
	free(databasename);
	return NULL;
}

void showDatabases(SimpleDatabase* dbms){
	printf("=================================\n");
	printf("数据库数量: %d\n", dbms->databaseMap->size);
	printf("数据库名\t表数量\n");
	foreachHashMap(dbms->databaseMap, printDatabaseItem, NULL);
	printf("\n\n");
}

void *printTableItem(struct Entry *entry, void *args){
	char* tablename = calloc(1, entry->keyLen+1);
	memcpy(tablename, entry->key, entry->keyLen);
	printf("%s\n", tablename);
	free(tablename);
	return NULL;
}

void showTables(SimpleDatabase* dbms,  const char *databasename){
	HashMap* tableMap = getHashMap(dbms->databaseMap, strlen(databasename), (uint8*)databasename);
	if(tableMap==NULL){
		printf("数据库 %s 不存在\n\n\n", databasename);
		return;
	}
	printf("=================================\n");
	printf("表数量: %d\n", tableMap->size);
	printf("表名\n");
	foreachHashMap(tableMap, printTableItem, NULL);
	printf("\n\n");
}

void *printFieldItem(void* value, void *args){
	FieldDefinition* field = (FieldDefinition*) value;
	printf("%s\t", field->name);
	char *type = NULL;
	if(field->type==FIELD_TYPE_INT){
		type = "int";
	} else if(field->type==FIELD_TYPE_UINT){
		type = "uint";
	} else if(field->type==FIELD_TYPE_STRING){
		type = "string";
	}
	printf("%s(%u)\t", type, field->length);
	char *key = NULL;
	if(field->flag==FIELD_FLAG_NORMAL){
		key = "nomarl";
	} else if(field->flag==FIELD_FLAG_PRIMARY_KEY){
		key = "primary key";
	} else if(field->flag==FIELD_FLAG_UNIQUE_KEY){
		key = "unique key";
	} else if(field->flag==FIELD_FLAG_INDEX_KEY){
		key = "index key";
	}
	printf("%s\n", key);
	return NULL;
}

void showFields(SimpleDatabase* dbms,  const char *databasename, const char *tablename){
	HashMap* tableMap = getHashMap(dbms->databaseMap, strlen(databasename), (uint8*)databasename);
	if(tableMap==NULL){
		printf("数据库 %s 不存在\n\n\n", databasename);
		return;
	}
	List *fields = getHashMap(tableMap, strlen(tablename), (uint8 *)tablename);
	if(fields==NULL){
		printf("表 %s 不存在\n\n\n", tablename);
		return;
	}
	printf("=================================\n");
	printf("字段名\t类型\t索引类型\n");
	foreachList(fields, printFieldItem, NULL);
	printf("\n\n");
}

void showRecord( List* fields, List* value){
	ListNode *node = fields->head;
	ListNode *node1 = value->head;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition*) node->value;
		void* fiedlValue = node1->value;
		if(field->type==FIELD_TYPE_STRING){
			//字符串类型
			printf("%s\t", (char *)fiedlValue);
		} else {
			if(field->length==1 || field->length==2 || field->length==4){
				if(field->type==FIELD_TYPE_INT){
					printf("%d\t", *(int32*) fiedlValue);
				} else if(field->type==FIELD_TYPE_UINT){
					printf("%u\t", *(uint32 *)fiedlValue);
				}
			} else if(field->length==8){
				if(field->type==FIELD_TYPE_INT){
					printf("%lld\t", *(int64 *)fiedlValue);
				} else if(field->type==FIELD_TYPE_UINT){
					printf("%llu\t", *(uint64 *)fiedlValue);
				}
			}
		}
		node = node->next;
		node1 = node1->next;
	}
	printf("\n");
}

void showRecords(SimpleDatabase* dbms,  List* fields, List* values){
	printf("=================================\n");
	ListNode *node = fields->head;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition*) node->value;
		printf("%s\t", field->name);
		node = node->next;
	}
	printf("\n");
	node = values->head;
	int recordLen = 0;
	while (node != NULL){
		recordLen++;
		List *value = (List *)node->value;
		showRecord(fields, value);
		node = node->next;
	}
	printf("----\n");
	printf("共查询到 %d 行\n", recordLen);
	printf("\n\n");
}

void cleanAndMakeDir(const char* path){
	if(fork() == 0){
		execl("/bin/rm", "rm", "-rf", path, NULL);
		exit(0);
	}
	sleep(1);
	if (fork() == 0){
		execl("/bin/mkdir", "mkdir", path, NULL);
		exit(0);
	}
	sleep(1);
}

void testInit(){
	char* path ="/tmp/dbms";
	cleanAndMakeDir(path);
	SimpleDatabase* dbms = makeSimpleDatabase(path);
	assertbool(dbms!=NULL, 1, "测试初始化");
}

void testCreateDatabase(){
	char *databasenames[] = {"database1", "test", "project1"};
	char *path = "/tmp/dbms";
	cleanAndMakeDir(path);
	SimpleDatabase *dbms = makeSimpleDatabase(path);
	for (int i = 0; i < sizeof(databasenames) / sizeof(databasenames[0]); i++){
		createDatabase(dbms, databasenames[i]);
	}
	showDatabases(dbms);
}

List* makeTestFieldList(){
	List *fields = makeList();
	//id
	FieldDefinition *id = malloc(sizeof(FieldDefinition));
	id->flag = FIELD_FLAG_PRIMARY_KEY;
	id->length = 8;
	id->name = "id";
	id->type = FIELD_TYPE_UINT;
	addList(fields, id);
	//title
	FieldDefinition *title = malloc(sizeof(FieldDefinition));
	title->flag = FIELD_FLAG_INDEX_KEY;
	title->length = 128;
	title->name = "title";
	title->type = FIELD_TYPE_STRING;
	addList(fields, title);
	//author
	FieldDefinition *author = malloc(sizeof(FieldDefinition));
	author->flag = FIELD_FLAG_NORMAL;
	author->length = 128;
	author->name = "author";
	author->type = FIELD_TYPE_STRING;
	addList(fields, author);
	//content
	FieldDefinition *content = malloc(sizeof(FieldDefinition));
	content->flag = FIELD_FLAG_NORMAL;
	content->length = 1024 * 1024;
	content->name = "content";
	content->type = FIELD_TYPE_STRING;
	addList(fields, content);
	//create_time
	FieldDefinition *create_time = malloc(sizeof(FieldDefinition));
	create_time->flag = FIELD_FLAG_NORMAL;
	create_time->length = 4;
	create_time->name = "create_time";
	create_time->type = FIELD_TYPE_UINT;
	addList(fields, create_time);
	//modify_time
	FieldDefinition *modify_time = malloc(sizeof(FieldDefinition));
	modify_time->flag = FIELD_FLAG_NORMAL;
	modify_time->length = 4;
	modify_time->name = "modify_time";
	modify_time->type = FIELD_TYPE_UINT;
	addList(fields, modify_time);
	return fields;
}

void testCreateTable(){
	char *path = "/tmp/dbms";
	char *databasename = "test";
	char *tablename = "article";
	List *fields = makeTestFieldList();
	cleanAndMakeDir(path);
	SimpleDatabase *dbms = makeSimpleDatabase(path);
	createDatabase(dbms, databasename);
	showDatabases(dbms);
	createTable(dbms, databasename, tablename, fields);
	showTables(dbms, databasename);
	showFields(dbms, databasename, tablename);
}

List* makeTestRecord(){
	List *values = makeList();
	//id
	uint64* id = malloc(sizeof(uint64));
	addList(values, id);
	*id = 1;
	//title
	char* title = "article title";
	addList(values, title);
	//authorc
	char* author = "rectcircle";
	addList(values, author);
	//content
	char* content = "article content";
	addList(values, content);
	//create_time
	uint32 *createTime = malloc(sizeof(uint32));
	*createTime = 1546324158u;
	addList(values, createTime);
	//modify_time
	uint32 *modifyTime = malloc(sizeof(uint32));
	*modifyTime = 1546324158u;
	addList(values, modifyTime);
	return values;
}



void testInsertAndQueryTable(){
	char *path = "/tmp/dbms";
	char *databasename = "test";
	char *tablename = "article";
	List *fields = makeTestFieldList();
	cleanAndMakeDir(path);
	SimpleDatabase *dbms = makeSimpleDatabase(path);
	createDatabase(dbms, databasename);
	showDatabases(dbms);
	createTable(dbms, databasename, tablename, fields);
	showTables(dbms, databasename);
	showFields(dbms, databasename, tablename);
	List* values = makeTestRecord();
	insertRecord(dbms, databasename,tablename, values);
	List* result = searchRecord(dbms, databasename, tablename, NULL);
	showRecords(dbms, fields, result);
}

TESTFUNC funcs[] = {
	testInit,
	testCreateDatabase,
	testCreateTable,
	testInsertAndQueryTable,
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs) / sizeof(TESTFUNC), funcs);
	return 0;
}
