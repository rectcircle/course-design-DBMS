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

void showDatabases(SimpleDatabase* database){
	printf("=================================\n");
	printf("数据库数量: %d\n", database->databaseMap->size);
	printf("数据库名\t表数量\n");
	foreachHashMap(database->databaseMap, printDatabaseItem, NULL);
	printf("\n\n");
}

void *printTableItem(struct Entry *entry, void *args){
	char* tablename = calloc(1, entry->keyLen+1);
	memcpy(tablename, entry->key, entry->keyLen);
	printf("%s\n", tablename);
	free(tablename);
	return NULL;
}

void showTables(SimpleDatabase* database,  const char *databasename){
	HashMap* tableMap = getHashMap(database->databaseMap, strlen(databasename), (uint8*)databasename);
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

void showFields(SimpleDatabase* database,  const char *databasename, const char *tablename){
	HashMap* tableMap = getHashMap(database->databaseMap, strlen(databasename), (uint8*)databasename);
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
	SimpleDatabase* database = makeSimpleDatabase(path);
	assertbool(database!=NULL, 1, "测试初始化");
}

void testCreateDatabase(){
	char *databasenames[] = {"database1", "test", "project1"};
	char *path = "/tmp/dbms";
	cleanAndMakeDir(path);
	SimpleDatabase *database = makeSimpleDatabase(path);
	for (int i = 0; i < sizeof(databasenames) / sizeof(databasenames[0]); i++){
		createDatabase(database, databasenames[i]);
	}
	showDatabases(database);
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
	modify_time->length = 8;
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
	SimpleDatabase *database = makeSimpleDatabase(path);
	createDatabase(database, databasename);
	showDatabases(database);
	createTable(database, databasename, tablename, fields);
	showTables(database, databasename);
	showFields(database, databasename, tablename);

}

TESTFUNC funcs[] = {
	testInit,
	testCreateDatabase,
	testCreateTable,
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs) / sizeof(TESTFUNC), funcs);
	return 0;
}
