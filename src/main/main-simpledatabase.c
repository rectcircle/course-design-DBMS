#include "simpledatabase.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>

//数据库管理系统
SimpleDatabase* dbms;
//当前使用的数据库
char* nowDatabaseName = NULL;
//数据存储目录
char* dirpath;
//当前命令
char command[65536];

typedef void (*HandleCommandFunction)(const char* command);

void showFirstTips(const char* dirpath){
	printf("Welcome to the  course-design-DBMS simple-database\n\n");
	printf("Your data will be saved to %s\n\n", dirpath);
	printf("Copyright (c) 2018~2019, Rectircle. All rights reserved.\n\n");
}

void showPrompt(){
	printf("\n>>> ");
}

void showUsage(){
	printf("Usage: main-simpledatabase <dirpath>\n");
	exit(1);
}

void showSyntaxError (const char* command){
	printf("`%s` has syntax error\n", command);
}


void showExecuteSuccess (const char* command){
	printf("`%s` execute success\n", command);
}

void checkDirpath(const char *dirpath){
	DIR *d; //声明一个句柄
	struct dirent *file; //readdir函数的返回值就存放在这个结构体中
	struct stat sb;

	if (!(d = opendir(dirpath)))
	{
		printf("error: %s open failed\n", dirpath);
		exit(1);
	}
	while ((file = readdir(d)) != NULL)
	{
		//把当前目录.，上一级目录..及隐藏文件都去掉，避免死循环遍历目录
		if (strncmp(file->d_name, ".", 1) == 0)
			continue;
		printf("error: %s is not an empty directory\n", dirpath);
		exit(1);
	}
	closedir(d);
}

void initDBMS(const char *dirpath){
	dbms = makeSimpleDatabase(dirpath);
	if(dbms==NULL){
		printf("error: init dbms failed\n");
		exit(1);
	}
}

void exitHandle(const char* command){
	exit(0);
}

void helpHandle(const char *command){
	printf("command list:\n");
	printf("help\tshow this text\n");
	printf("exit\texit\n");
	printf("quit\tsame as exit\n");
	printf("\n");
	printf("support sql list:\n");
	printf("show databases\n");
	printf("create database <database_name>\n");
	printf("use <database_name>\n");
	printf("show tables\n");
	printf("create table <table_name> (<field_name> <uint|int|string>(<length>) [<unique|primary|index>] [,...])\n");
	printf("desc <table_name>\n");
	printf("insert into <table_name> values(...)\n");
	printf("select * from <table_name> where <field_name> <=|!=|<|<=|>|>=> <value> [and ...]\n");
}

void nothing(const char *command){
	return;
}

void commadError(const char *command){
	printf("`%s` is not supported command or sql\n", command);
}

void showTableStringItem(int len, const char* item){
	char *str = calloc(1, len+1);
	int itemLen = strlen(item);
	if(itemLen > len){
		memcpy(str, item, len);
		str[itemLen-1] = ' ';
		for(int i = itemLen-2; i>=itemLen-4; i--){
			str[i] = '.';
		}
	} else if(itemLen == len){
		memcpy(str, item, len);
	} else {
		memcpy(str, item, itemLen);
		for(int i = itemLen; i<len; i++){
			str[i] = ' ';
		}
	}
	printf("%s|", str);
	free(str);
}

void showTableUintItem(int len, const uint64 item){
	char * str = malloc(20);
	sprintf(str, "%llu", item);
	showTableStringItem(len, str);
	free(str);
}

void showTableIntItem(int len, const int64 item){
	char * str = malloc(20);
	sprintf(str, "%lld", item);
	showTableStringItem(len, str);
	free(str);
}

void *printDatabaseItem(struct Entry *entry, void *args){
	char *col1 = " database name ";
	char *col2 = " table total ";
	int databaseNameLen = strlen(col1);
	int tableTotalLen = strlen(col2);
	char* databasename = calloc(1, entry->keyLen+1);
	memcpy(databasename, entry->key, entry->keyLen);
	printf("|");
	showTableStringItem(databaseNameLen, databasename);
	showTableUintItem(tableTotalLen, ((HashMap*)entry->value)->size);
	printf("\n");
	free(databasename);
	return NULL;
}

char* getWordByIndex(char *command, int index){
	int wordIdx = -1;
	int spaceFlag = 1;
	int startIndex = -1;
	int wordLen = 0;
	for(int i=0; i<strlen(command); i++){
		if(command[i]==' ' || command[i]=='\t' || command[i]=='\n'){
			if(spaceFlag == 0){
				if(index==wordIdx){
					wordLen = i - startIndex;
					break;
				}
			}
			spaceFlag = 1;
		} else {
			if(spaceFlag == 1){
				wordIdx++;
				if(index==wordIdx){
					startIndex = i;
				}
			}
			spaceFlag = 0;
		}
	}
	char *word = NULL;
	if(startIndex != -1){
		if(wordLen==0){
			wordLen = strlen(command) - startIndex;
		}
		word = malloc(wordLen + 1);
		memcpy(word, command+startIndex, wordLen);
		word[wordLen] = '\0';
	}	
	return word;
}


void createDatabaseHandle(const char *command){
	char* databaseName = getWordByIndex(command, 2);
	if(databaseName==NULL){
		showSyntaxError(command);
		return;
	}
	if(createDatabase(dbms, databaseName)){
		showExecuteSuccess(command);
	}
}

void showDatabaseHandle(const char *command){
	char *col1 = " database name ";
	char *col2 = " table total ";
	int databaseNameLen = strlen(col1);
	int tableTotalLen = strlen(col2);

	printf("|");
	showTableStringItem(databaseNameLen, col1);
	showTableStringItem(tableTotalLen, col2);
	printf("\n");
	printf(dbms->databaseMap);
	foreachHashMap(dbms->databaseMap, printDatabaseItem, NULL);
	printf("total: %d\n", dbms->databaseMap->size);
}

void useDatabaseHandle(const char *command){
	char *databasename = getWordByIndex(command, 1);
	if(databasename==NULL){
		showSyntaxError(command);
		return;
	}
	if(NULL==getHashMap(dbms->databaseMap, strlen(databasename), databasename)){
		printf("error: database `%s` not exist\n", databasename);
		return;
	}
	nowDatabaseName = databasename;
	showExecuteSuccess(command);
}

void *printTableItem(struct Entry *entry, void *args){
	char *col1 = "table name ";
	int tableNameLen = strlen(col1);
	char* tablename = calloc(1, entry->keyLen+1);
	memcpy(tablename, entry->key, entry->keyLen);
	printf("|");
	showTableStringItem(tableNameLen, tablename);
	printf("\n");
	free(tablename);
	return NULL;
}

void showTableHandle(const char *command){
	if(nowDatabaseName==NULL){
		printf("error: please select database by `use <database_name>` first\n");
		return;
	}
	HashMap *tableMap = getHashMap(dbms->databaseMap, strlen(nowDatabaseName), (uint8 *)nowDatabaseName);
	if(tableMap==NULL){
		printf("error: database `%s` not exist\n", nowDatabaseName);
		return;
	}
	printf("|");
	char *col1 = "table name ";
	int tableNameLen = strlen(col1);
	showTableStringItem(tableNameLen, col1);
	printf("\n");
	foreachHashMap(tableMap, printTableItem, NULL);
	printf("total: %d\n", tableMap->size);
}

//获取字段定义右括号位置
static int getFirstLeftParenthesisIndex(const char *command){
	for(int i=0; i<strlen(command); i++){
		if(command[i]=='('){
			return i;
		}
	}
	return -1;
}

static int getLastRightParenthesisIndex(const char *command){
	for(int i=strlen(command)-1; i>=0; i--){
		if(command[i]==')'){
			return i;
		}
	}
	return -1;
}

static List* splitFieldListString(const char* fieldListString, char splitChar){
	List* fieldStringList = makeList();
	int start = 0;
	for(int i=0; i<=strlen(fieldListString); i++){
		if(fieldListString[i] == splitChar || (i==strlen(fieldListString) && i>start)){
			char * fieldString = malloc(i-start+1);
			memcpy(fieldString, fieldListString + start, i - start);
			fieldString[i-start] = '\0';
			addList(fieldStringList, fieldString);
			start = i+1;
		}
	}
	return fieldStringList;
}


static FieldDefinition* parseField(const char* string){

	char* fieldName = getWordByIndex(string, 0);
	char *typeAndLen = getWordByIndex(string, 1);
	if(fieldName==NULL||typeAndLen==NULL){
		showSyntaxError(string);
		return NULL;
	}
	char *constraint = getWordByIndex(string, 2);


 	FieldDefinition *field = malloc(sizeof(FieldDefinition));
	//设置名字
	field->name = fieldName;
	uint32 fieldLen = 0;

	int splitIdx = getFirstLeftParenthesisIndex(typeAndLen);
	char *fieldTypeName = calloc(1, splitIdx+1);
	memcpy(fieldTypeName, typeAndLen, splitIdx);

	sscanf(typeAndLen+splitIdx+1, "%u)", &fieldLen);
	//设定类型
	//设定长度
	if(strcmp(fieldTypeName, "string") == 0){
		field->type = FIELD_TYPE_STRING;
	} else if(strcmp(fieldTypeName, "uint") == 0){
		field->type = FIELD_TYPE_UINT;
	} else if(strcmp(fieldTypeName, "int") == 0){
		field->type = FIELD_TYPE_INT;
	} else {
		showSyntaxError(string);
		return NULL;
	}
	field->length = fieldLen;
	if (field->type != FIELD_TYPE_STRING){
		if (field->length != 1 && field->length != 2 && field->length != 4 && field->length != 8){
			showSyntaxError("number type length must in (1, 2, 4, 8)");
			return NULL;
		}
	}
	//设置约束
	if(constraint==NULL){
		field->flag = FIELD_FLAG_NORMAL;
	} else {
		if (strcmp(constraint, "primary")==0){
			field->flag = FIELD_FLAG_PRIMARY_KEY;
		} else if(strcmp(constraint, "unique")==0){
			field->flag = FIELD_FLAG_UNIQUE_KEY;
		} else if(strcmp(constraint, "index")==0){
			field->flag = FIELD_FLAG_INDEX_KEY;
		} else {
			showSyntaxError("constraint must in (primary, unique, index)");
			return NULL;
		}
	}
	return field;
}

//create table article(id uint(8) primary, title string(256) index, author string(128), content string(1048576),  create_time uint(4), modify_time uint(4))
void createTableHandle(const char *command){
	int commandLen = strlen(command);
	int leftParenthesisIndex = getFirstLeftParenthesisIndex(command);
	int lastRightParenthesisIndex = getLastRightParenthesisIndex(command);
	if(leftParenthesisIndex==-1){
		showSyntaxError(command);
	}
	char* leftString = malloc(leftParenthesisIndex+1);
	memcpy(leftString, command, leftParenthesisIndex);
	leftString[leftParenthesisIndex] = '\0';
	char* tablename = getWordByIndex(leftString, 2);
	if(tablename==NULL){
		showSyntaxError(command);
	}

	char *fieldListString = malloc(lastRightParenthesisIndex - leftParenthesisIndex);
	memcpy(fieldListString, command + leftParenthesisIndex + 1, lastRightParenthesisIndex - leftParenthesisIndex-1);
	fieldListString[commandLen - leftParenthesisIndex-1] = '\0';
	List *fieldStringList = splitFieldListString(fieldListString, ',');

	List* fieldList = makeList();
	ListNode* node = fieldStringList->head;
	int primarySum = 0;
	while(node->next!=NULL){
		char* fieldString = (char*)node->value;
		// printf("%s\n", fieldString);
		FieldDefinition* field = parseField(fieldString);
		if(field==NULL){
			return;
		}
		if(field->flag == FIELD_FLAG_PRIMARY_KEY){
			primarySum++;
		}
		addList(fieldList, field);
		node = node->next;
	}
	if(primarySum!=1){
		showSyntaxError("primary key must exist and only exist one");
	}
	if(createTable(dbms, nowDatabaseName, tablename, fieldList)!=1){
		printf("error: unknown\n");
	} else {
		printf("create table `%s` success\n", tablename);
	}
}

static void *printFieldItem(void* value, void *args){
	FieldDefinition* field = (FieldDefinition*) value;
	printf("|");
	showTableStringItem(15, field->name);
	char *type = NULL;
	if(field->type==FIELD_TYPE_INT){
		type = "int";
	} else if(field->type==FIELD_TYPE_UINT){
		type = "uint";
	} else if(field->type==FIELD_TYPE_STRING){
		type = "string";
	}
	showTableStringItem(8, type);
	showTableUintItem(10, field->length);
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
	showTableStringItem(15, key);
	printf("\n");
	return NULL;
}

void showColumnsHandle(const char *command){
	char* tablename = getWordByIndex(command, 1);
	HashMap *tableMap = getHashMap(dbms->databaseMap, strlen(nowDatabaseName), (uint8 *)nowDatabaseName);
	List *fields = getHashMap(tableMap, strlen(tablename), (uint8 *)tablename);
	if(fields==NULL){
		printf("table `%s` not exist\n", tablename);
		return;
	}
	printf("|");
	showTableStringItem(15, "field name");
	showTableStringItem(8, "type");
	showTableStringItem(10, "length");
	showTableStringItem(15, "key");
	printf("\n");
	foreachList(fields, printFieldItem, NULL);
}

void insertTableHandle(const char *command){

}

void selectTableHandle(const char *command){

}

struct CommandToHandler {
	char *prefix;
	HandleCommandFunction handle;
};

struct CommandToHandler handles[] = {
	{"exit", exitHandle},
	{"quit", exitHandle},
	{"help", helpHandle},

	{"show databases", showDatabaseHandle},
	{"create database", createDatabaseHandle},
	{"use", useDatabaseHandle},
	{"show tables", showTableHandle},
	{"create table", createTableHandle},
	{"desc", showColumnsHandle},
	{"insert into", insertTableHandle},
	{"select * from", selectTableHandle},
};

HandleCommandFunction matchCommand(const char *command){
	int startIndex=-1;
	for(int i=0; i<strlen(command); i++){
		if(command[i]!=' ' && command[i]!='\t' && command[i]!='\n'){
			startIndex = i;
			break;
		}
	}
	if(startIndex==-1){
		return nothing;
	}
	for(int i=0; i<sizeof(handles)/sizeof(handles[0]); i++){
		const char* prefix = handles[i].prefix;
		if(strlen(command)-startIndex>=strlen(prefix) && 
			strncmp(command+startIndex, prefix, strlen(prefix))==0){
				return handles[i].handle;
		}
	}
	return commadError;
}

void repl(){
	while(1){
		showPrompt();
		fgets(command, sizeof(command), stdin);
		command[strlen(command)-1] = '\0';
		HandleCommandFunction handle = matchCommand(command);
		handle(command);
	}
}

int main(int argc, char const *argv[]){
	if(argc!=2){
		showUsage();
	}
	dirpath = argv[1];
	checkDirpath(dirpath);
	initDBMS(dirpath);
	showFirstTips(dirpath);
	repl();
	return 0;
}
