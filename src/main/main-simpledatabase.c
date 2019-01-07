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

char* getWordByIndex(const char *command, int index){
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
	foreachHashMap(dbms->databaseMap, printDatabaseItem, NULL);
	printf("total: %d\n", dbms->databaseMap->size);
}

void useDatabaseHandle(const char *command){
	char *databasename = getWordByIndex(command, 1);
	if(databasename==NULL){
		showSyntaxError(command);
		return;
	}
	if(NULL==getHashMap(dbms->databaseMap, strlen(databasename), (uint8*)databasename)){
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

static int getFirstCharIndex(const char* command, char c){
	for(int i=0; i<strlen(command); i++){
		if(command[i]==c){
			return i;
		}
	}
	return -1;
}

static int getFirstStringIndex(const char* command, const char* sub){
	int subLen = strlen(sub);
	int commandLen = strlen(command);
	for(int i=0; i<commandLen-subLen+1; i++){
		if(strncmp(command+i, sub, subLen)==0){
			return i;
		}
	}
	return -1;
}

//获取字段定义右括号位置
static int getFirstLeftParenthesisIndex(const char *command){
	return getFirstCharIndex(command, '(');
}

static int getLastCharIndex(const char* command, char c){
	for(int i=strlen(command)-1; i>=0; i--){
		if(command[i]==c){
			return i;
		}
	}
	return -1;
}


static int getLastRightParenthesisIndex(const char *command){
	return getLastCharIndex(command, ')');
}

static List* splitByChar(const char* fieldListString, char splitChar){
	List* fieldStringList = makeList();
	int start = 0;
	int originLen = strlen(fieldListString);
	for(int i=0; i<=strlen(fieldListString); i++){
		if((i!= originLen && fieldListString[i] == splitChar) || (i==strlen(fieldListString) && i>start)){
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
	if(nowDatabaseName==NULL){
		printf("error: please select database by `use <database_name>` first\n");
		return;
	}
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
	List *fieldStringList = splitByChar(fieldListString, ',');

	List* fieldList = makeList();
	ListNode* node = fieldStringList->head;
	int primarySum = 0;
	while(node!=NULL){
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

static void printFieldItem(void* value, void *args){
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
}

void showColumnsHandle(const char *command){
	if(nowDatabaseName==NULL){
		printf("error: please select database by `use <database_name>` first\n");
		return;
	}
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

void *stringToValue(FieldDefinition *field, char *valueString){
	if(field->type==FIELD_TYPE_STRING){
		int leftIndex = getFirstCharIndex(valueString, '\'');
		int rightIndex = getLastCharIndex(valueString, '\'');

		int len = rightIndex - leftIndex - 1;
		char *value = malloc(len+1);
		value[len] = '\0';
		memcpy(value, valueString + leftIndex+1, len);
		return value;
	} else if(field->type==FIELD_TYPE_UINT){
		uint64 tmp;
		sscanf(valueString, "%llu", &tmp);
		if(field->length==1){
			uint8* value = malloc(sizeof(uint8));
			*value = tmp;
			return value;
		} else if(field->length==2){
			uint16 *value = malloc(sizeof(uint16));
			*value = tmp;
			return value;
		} else if(field->length==4){
			uint32 *value = malloc(sizeof(uint32));
			*value = tmp;
			return value;
		} else if(field->length==8){
			uint64 *value = malloc(sizeof(uint64));
			*value = tmp;
			return value;
		}
	} else if(field->type==FIELD_TYPE_INT){
		int64 tmp;
		sscanf(valueString, "%lld", &tmp);
		if(field->length==1){
			int8 *value = malloc(sizeof(int8));
			*value = tmp;
			return value;
		} else if(field->length==2){
			int16 *value = malloc(sizeof(int16));
			*value = tmp;
			return value;
		} else if(field->length==4){
			int32 *value = malloc(sizeof(int32));
			*value = tmp;
			return value;
		} else if(field->length==8){
			int64 *value = malloc(sizeof(int64));
			*value = tmp;
			return value;
		}
	}
	return NULL;
}

// insert into article values(1, 'article title', 'rectcircle', 'article content', 1546324158, 1546324158);
void insertTableHandle(const char *command){
	if(nowDatabaseName==NULL){
		printf("error: please select database by `use <database_name>` first\n");
		return;
	}
	char* tablename = getWordByIndex(command, 2);
	HashMap *tableMap = getHashMap(dbms->databaseMap, strlen(nowDatabaseName), (uint8 *)nowDatabaseName);
	List *fields = getHashMap(tableMap, strlen(tablename), (uint8 *)tablename);
	if(fields==NULL){
		printf("table `%s` not exist\n", tablename);
		return;
	}

	int leftParenthesisIndex = getFirstLeftParenthesisIndex(command);
	int rightParenthesisIndex = getLastRightParenthesisIndex(command);
	if(leftParenthesisIndex == -1 || rightParenthesisIndex==-1){
		showSyntaxError(command);
		return;
	}
	char* valueListString = malloc(rightParenthesisIndex-leftParenthesisIndex);
	valueListString[rightParenthesisIndex-leftParenthesisIndex] = '\0';
	memcpy(valueListString, command + leftParenthesisIndex+1, rightParenthesisIndex - leftParenthesisIndex-1);
	List* splitList = splitByChar(valueListString, ',');
	if(splitList->length!=fields->length){
		printf("error: value length must equals to colums length\n");
	}
	
	List* valueList = makeList();

	ListNode* node = fields->head;
	ListNode* node1 = splitList->head;
	while(node!=NULL){
		FieldDefinition* field = (FieldDefinition*)node->value;
		char* valueString = (char*)node1->value;
		void *value = stringToValue(field, valueString);
		if(value==NULL){
			showSyntaxError(valueString);
			return;
		}
		addList(valueList, value);
		node = node->next;
		node1 = node1->next;
	}
	if(insertRecord(dbms, nowDatabaseName, tablename, valueList)==1){
		printf("insert into `%s` success\n", tablename);
	}
}


void showRecord( List* fields, List* value, int* showLens){
	ListNode *node = fields->head;
	ListNode *node1 = value->head;
	int i=0;
	printf("|");
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition*) node->value;
		void* fiedlValue = node1->value;
		if(field->type==FIELD_TYPE_STRING){
			//字符串类型
			showTableStringItem(showLens[i], (char *)fiedlValue);
		} else if(field->type==FIELD_TYPE_UINT){
			uint64 value;
			if(field->length==1){
				value = *(uint8 *)fiedlValue;
			} else if(field->length==2){
				value = *(uint16 *)fiedlValue;
			} else if(field->length==4){
				value = *(uint32 *)fiedlValue;
			} else if(field->length==8){
				value = *(uint64 *)fiedlValue;
			}
			showTableUintItem(showLens[i], value);
		} else if(field->type==FIELD_TYPE_INT){
			int64 value;
			if(field->length==1){
				value = *(int8 *)fiedlValue;
			} else if(field->length==2){
				value = *(int16 *)fiedlValue;
			} else if(field->length==4){
				value = *(int32 *)fiedlValue;
			} else if(field->length==8){
				value = *(int64 *)fiedlValue;
			}
			showTableIntItem(showLens[i], value);
		}
		node = node->next;
		node1 = node1->next;
		i++;
	}
	printf("\n");
}

static List* splitByString(const char* origin, char* splitStr){
	List* result = makeList();
	int start = 0;
	int originLen = strlen(origin);
	int splitLen = strlen(splitStr);
	for(int i=0; i<=originLen - splitLen+1;){
		if((i!=originLen - splitLen+1 && strncmp(splitStr, origin+i, splitLen) == 0) || (i==originLen - splitLen+1)){
			int len = i-start;
			if(i==originLen - splitLen+1){
				len = originLen - start;
			}
			char *value = malloc(len + 1);
			memcpy(value, origin + start, len);
			value[len] = '\0';
			addList(result, value);
			start = i+splitLen;
			i += splitLen;
		} else {
			i += 1;
		}
	}
	return result;
}

List* genQueryConditions(List *fields, const char *command){
	List* result = makeList();
	List* conditionStringList = splitByString(command, "and");
	ListNode* node = conditionStringList->head;
	while(node!=NULL){
		QueryCondition *condition = malloc(sizeof(QueryCondition));

		char* kv =  (char*)node->value;
		int splitIndex = -1;
		int splitLen = 1;

		if(splitIndex==-1){
			splitIndex = getFirstStringIndex(kv, "!=");
			splitLen = 2;
			condition->relOp = RELOP_NEQ;
		}
		if (splitIndex == -1){
			splitIndex = getFirstStringIndex(kv, "<=");
			splitLen = 2;
			condition->relOp = RELOP_LTE;
		}
		if (splitIndex == -1){
			splitIndex = getFirstStringIndex(kv, "<");
			splitLen = 1;
			condition->relOp = RELOP_LT;
		}
		if (splitIndex == -1){
			splitIndex = getFirstStringIndex(kv, ">=");
			splitLen = 2;
			condition->relOp = RELOP_GTE;
		}
		if (splitIndex == -1){
			splitIndex = getFirstStringIndex(kv, ">");
			splitLen = 1;
			condition->relOp = RELOP_GT;
		}
		if (splitIndex==-1){
			splitIndex = getFirstCharIndex(kv, '=');
			splitLen = 1;
			condition->relOp = RELOP_EQ;
		}
		if(splitIndex==-1){
			showSyntaxError(command);
			return NULL;
		}
		kv[splitIndex] = '\0';
		char* key = getWordByIndex(kv, 0);
		FieldDefinition* field = getFieldByName(fields, key);
		void *value = stringToValue(field,kv+splitIndex+splitLen);

		condition->name = key;
		condition->logOp = LOGOP_AND;
		condition->value = value;
		addList(result, condition);
		node = node->next;
	}
	return result;
}

void selectTableHandle(const char *command){
	if(nowDatabaseName==NULL){
		printf("error: please select database by `use <database_name>` first\n");
		return;
	}
	char* tablename = getWordByIndex(command, 3);
	HashMap *tableMap = getHashMap(dbms->databaseMap, strlen(nowDatabaseName), (uint8 *)nowDatabaseName);
	List *fields = getHashMap(tableMap, strlen(tablename), (uint8 *)tablename);
	if(fields==NULL){
		printf("table `%s` not exist\n", tablename);
		return;
	}

	List *conds = NULL;
	int whereIdx = getFirstStringIndex(command, "where");
	if(whereIdx != -1){
		conds = genQueryConditions(fields, command + whereIdx + strlen("where")+1);
		if(conds==NULL){
			return;
		}
	}
	List* result = searchRecord(dbms, nowDatabaseName, tablename, conds);

	//输出列名
	printf("|");
	ListNode* node = fields->head;
	int* lenArr = malloc(sizeof(int)*fields->length);
	int i=0;
	while(node!=NULL){
		FieldDefinition* field = (FieldDefinition*)node->value;
		if(field->length<10){
			lenArr[i] = 10;
		} else if(field->length>=30){
			lenArr[i] = 30;
		} else {
			lenArr[i] = field->length+1;
		}
		if(lenArr[i]<=strlen(field->name)){
			lenArr[i] = strlen(field->name)+1;
		}
		showTableStringItem(lenArr[i], field->name);
		i++;
		node = node->next;
	}
	printf("\n");

	node = result->head;
	int recordLen = 0;
	i=0;
	while (node != NULL)
	{
		recordLen++;
		List *value = (List *)node->value;
		showRecord(fields, value, lenArr);
		node = node->next;
	}
	printf("tatol: %d\n", recordLen);
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

int main(int argc, char *argv[]){
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
