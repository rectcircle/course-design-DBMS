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
char* nowDatabaseName;
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
	printf("exit\t exit\n");
	printf("quit\t same as exit\n");
	printf("\n");
	printf("support sql list:\n");
	printf("show databases\n");
	printf("create database <database_name>\n");
	printf("use <database_name>\n");
	printf("show tables\n");
	printf("create table <table_name> (<field_name> <uint|int|string>(<length>) [<unique|primary|index>] [,...])\n");
	printf("insert into <table_name> values(...)\n");
	printf("select * from <table_name> where <field_name> <=|!=|<|<=|>|>=> <value> [and ...]\n");
}

void nothing(const char *command){
	return;
}

void commadError(const char *command){
	printf("`%s` is not supported command or sql\n", command);
}

void createDatabaseHandle(const char *command){

}

void showDatabaseHandle(const char *command){

}

void useDatabaseHandle(const char *command){

}

void showTableHandle(const char *command){

}

void createTableHandle(const char *command){

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
