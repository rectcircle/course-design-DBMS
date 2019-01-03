/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * 简单整合
 * 
 * @filename: simpledatabase.h
 * @description: 简单数据库实现
 * @author: Rectcircle
 * @version: 1.0
 * @date 2018-12-30 
 ******************************************************************************/
#pragma once
#ifndef __SIMPLEDATABASE_H__
#define __SIMPLEDATABASE_H__

#include "hashengine.h"
#include "indexengine.h"

/** 无符号整形 */
#define FIELD_TYPE_UINT 1
/** 有符号整形 */
#define FIELD_TYPE_INT 2
/** 字符类型 */
#define FIELD_TYPE_STRING 3

/** 普通字段 */
#define FIELD_FLAG_NORMAL 0
/** 主键字段 */
#define FIELD_FLAG_PRIMARY_KEY 1
/** 唯一索引字段 */
#define FIELD_FLAG_UNIQUE_KEY 2
/** 普通索引字段 */
#define FIELD_FLAG_INDEX_KEY 3

/** 逻辑且 */
#define LOGOP_AND 0
/** 逻辑或 */
#define LOGOP_OR 1

/** 字段定义 */
typedef struct FieldDefinition
{
	/** 字段类型 */
	uint8 type;
	/** 字段长度 */
	uint32 length;
	/** 是否是索引 */
	uint32 flag;
	/** 字段名 */
	char *name;
} FieldDefinition;

/** 查询条件定义 */
typedef struct QueryCondition{
	/** 字段名 */
	char *name;
	/** 关系运算符 */
	uint8 relOp;
	/** 比较值 */
	void *value;
	/** 逻辑运算符 */
	uint8 logOp;
} QueryCondition;

typedef struct SimpleDatabase
{
	/** 元数据: 文件名为metadata.hashengine */
	/** HashMap<数据库名, HashMap<表名, List<FieldDefinition>>> */
	struct HashMap* databaseMap;
	/** HashMap<表文件名, HashEngine> */
	struct HashMap* dataMap;
	/** HashMap<索引表文件名, indexEngine> */
	struct HashMap *indexMap;
	/** 数据文件目录 */
	char *dirpath;
} SimpleDatabase;

/**
 * 创建一个简单整合的数据库
 * @param dirpath 数据存储目录
 * @return 简单数据库
 */
SimpleDatabase *makeSimpleDatabase(const char *dirpath);

/**
 * 创建一个数据库
 * @param dbms
 * @param databasename 数据库名
 * @return 创建成功返回1,否则返回0
 */
int createDatabase(SimpleDatabase *dbms, const char *databasename);

/**
 * 创建一张表
 * @param dbms
 * @param databasename 数据库名
 * @param tablename 表名
 * @param fields 字段列表
 */
int createTable(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *fields);

/**
 * 获取表定义
 */
List *getFieldDefinitions(SimpleDatabase *dbms, const char *databasename, const char *tablename);

/**
 * 向表中插入一条数据
 * @param dbms
 * @param databasename 数据库名
 * @param tablename 表名
 * @param values 记录列表 List<void*>
 */
int insertRecord(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *values);

/**
 * 从表中查询记录
 * TODO 目前仅支持单条件, 索引方式精确查找 和 查询全部
 * @param dbms
 * @param databasename 数据库名
 * @param tablename 表名
 * @param conditions 条件列表List<QueryCondition*>, NULL表示查询全部
 * @return 值列表List<List<void*>>
 */
List* searchRecord(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *conditions);

#endif
