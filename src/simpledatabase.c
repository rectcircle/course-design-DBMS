/*****************************************************************************
 * Copyright (c) 2018, rectcircle. All rights reserved.
 * 
 * @filename: simpledatabase.c 
 * @description: 简单数据库实现
 * @author: Rectcircle
 * @version: 1.0
 * @date: 2018-12-08
 ******************************************************************************/
#include <malloc.h>
#include <string.h>

#include "simpledatabase.h"

static const char* METADATA_KEY = "metadata.hashengine";


// ${dirpath}/${filename}
static char * genFullpath(const char* dirpath, const char* filename){
	char *filepath = malloc(strlen(dirpath) + 1 + strlen(filename) + 1);
	sprintf(filepath, "%s/%s", dirpath, filename);
	return filepath;
}

// ${dirpath}/metadata.hashengine
static char * genMetadatapath(const char* dirpath){
	return genFullpath(dirpath, METADATA_KEY);
}

// ${databasename}_${tablename}.hashengine
static char * genTablefilename(const char* databasename, const char* tablename){
	char *filename = malloc(strlen(databasename) + 1 + strlen(tablename) + strlen(".hashengine") + 1);
	sprintf(filename, "%s_%s.hashengine", databasename, tablename);
	return filename;
}

// ${databasename}_${tablename}_${field}.indexengine
static char * genIndexfilename(const char*databasename, const char* tablename, const char* fieldname){
	char *filename = malloc(strlen(databasename) + 1 + strlen(tablename) + 1 + strlen(fieldname) + strlen(".indexengine")+1);
	sprintf(filename, "%s_%s_%s.indexengine", databasename, tablename, fieldname);
	return filename;
}

SimpleDatabase *makeSimpleDatabase(const char *dirpath){
	SimpleDatabase* database = malloc(sizeof(SimpleDatabase));
	newAndCopyByteArray((uint8**)&database->dirpath, (uint8*)dirpath, strlen(dirpath)+1);
	int hashMapSize = 1024;
	int metadataHashMapCap = 1024;
	int metadataCacheCap = 1024;
	database->databaseMap = makeHashMap(hashMapSize);
	database->dataMap = makeHashMap(hashMapSize);
	database->indexMap = makeHashMap(hashMapSize);
	char *metadataPath = genMetadatapath(dirpath);
	HashEngine* metadateHashEngine = makeHashEngine(metadataPath, metadataCacheCap, metadataHashMapCap, 1024, sizeThreshold, 1024);
	putHashMap(database->dataMap, strlen(METADATA_KEY), (uint8*)METADATA_KEY,metadateHashEngine);

	free(metadataPath);
	return database;
}

int createDatabase(SimpleDatabase *database, const char *databasename){
	int hashMapCap = 1024;
	if(getHashMap(database->databaseMap, strlen(databasename), (uint8*) databasename)!=NULL){
		printf("数据库 %s 已存在", databasename);
		return 0;
	}
	HashMap *tableMap = makeHashMap(hashMapCap);
	putHashMap(database->databaseMap, strlen(databasename), (uint8*)databasename, tableMap);
	return 1;
}

int createTable(SimpleDatabase *database, const char *databasename, const char *tablename, List *fields){
	int dataHashMapCap = 1024;
	int dataCacheCap = 1024;
	HashMap *tableMap = (HashMap*)getHashMap(database->databaseMap, strlen(databasename), (uint8*)databasename);
	if(tableMap==NULL){
		printf("请先创建数据库\n");
		return 0;
	}
	//检测每个字段是否合法, 并找到主键
	ListNode* node = fields->head;
	FieldDefinition* primaryKeyField = NULL;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition*)node->value;
		if(field->flag==FIELD_FLAG_PRIMARY_KEY && primaryKeyField==NULL){
			primaryKeyField = field;
			break;
		}
		if(field->type!=FIELD_TYPE_STRING){
			if (field->length != 1 && field->length != 2 && field->length != 4 && field->length != 8){
				printf("数字类型长度可选1,2,4,8字节");
				return 0;
			}
		}
		node = node->next;
	}
	if(primaryKeyField==NULL){
		printf("表必须包含主键\n");
		return 0;
	}
	//放到Metadata中
	putHashMap(tableMap, strlen(tablename), (uint8*)tablename, fields);
	//创建数据文件${databaseName}_${tableName}_table.hashengine
	char * tableFilename = genTablefilename(databasename, tablename);
	char * tableFilepath = genFullpath(database->dirpath, tableFilename);
	//创建HashEngine
	HashEngine *tableData = makeHashEngine(tableFilepath, dataHashMapCap, dataCacheCap, 1024, sizeThreshold, 1024);
	putHashMap(database->dataMap, strlen(tableFilename), (uint8 *)tableFilename, tableData);
	//循环创建索引文件
	uint64 pageSize = 16*1024;
	uint64 maxHeapSize = 0; //默认值
	node = fields->head;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition*)node->value;
		if(field->flag==FIELD_FLAG_NORMAL){
			node = node->next;
			continue;
		}
		int8 isUnique = 1;
		if(field->flag==FIELD_FLAG_INDEX_KEY){
			isUnique = 0;
		}
		char *indexFilename = genIndexfilename(databasename, tablename, field->name);
		char *indexFilepath = genFullpath(database->dirpath, indexFilename);
		// 创建索引引擎
		IndexEngine *tableIndex = makeIndexEngine(
			indexFilepath,
			field->length,
			primaryKeyField->length,
			pageSize,
			isUnique,
			maxHeapSize,
			1024, sizeThreshold, 1024);
		putHashMap(database->indexMap, strlen(indexFilename), (uint8 *)indexFilename, tableIndex);
		free(indexFilename);
		free(indexFilepath);
		node = node->next;
	}
	free(tableFilename);
	free(tableFilepath);
	return 1;
}

//检查数据是否合法
//将List<void*> -> List<Array{length, bytes}>, 涉及字节序转换 
static List *checkAndDumpValues(List* fields, List* values){
	//TODO 内存泄露
	List* result = makeList();
	if (values->length != fields->length)
	{
		printf("插入数据字段数与表定义长度不一致\n");
		return NULL;
	}
	ListNode* node1 = fields->head;
	ListNode *node2 = values->head;
	while (node1 != NULL) {
		FieldDefinition *field = (FieldDefinition*)node1->value;
		void *value = node2->value;
		Array* data = malloc(sizeof(Array));
		if(field->type==FIELD_TYPE_STRING){
			data->length = strlen((char*)value);
			if(data->length>field->length){
				printf("%s 字段的值长度大于字段定义的长度\n", field->name);
				return NULL;
			}
			newAndCopyByteArray((uint8**)&data->array, (uint8*)value, data->length);
		} else {
			data->length = field->length;
			if (field->length==1){
				newAndCopyByteArray((uint8 **)&data->array, (uint8*)value, data->length);
			} else if(field->length==2){
				uint16 hostNumber = *(uint16 *)value;
				uint16 networkNumber = htons(hostNumber);
				newAndCopyByteArray((uint8 **)&data->array, (uint8*)&networkNumber, data->length);
			} else if(field->length==4){
				uint32 hostNumber = *(uint32 *)value;
				uint32 networkNumber = htonl(hostNumber);
				newAndCopyByteArray((uint8 **)&data->array, (uint8 *)&networkNumber, data->length);
			} else if (field->length == 8) {
				uint64 hostNumber = *(uint64 *)value;
				uint64 networkNumber = htonll(hostNumber);
				newAndCopyByteArray((uint8 **)&data->array, (uint8 *)&networkNumber, data->length);
			}
		}
		addList(result, data);
		node1 = node1->next;
		node2 = node2->next;
	}
	return result;
}

static Array* dumpvaluesToBuffer(int32 len, List* fields, List*dumpvalues){
	uint8* buffer = malloc(len);
	Array* result = malloc(sizeof(Array));
	result->length = len;
	result->array = buffer;
	ListNode* node = dumpvalues->head;
	ListNode *node1 = fields->head;
	while (node != NULL) {
		Array* value = (Array*) node->value;
		FieldDefinition* field = (FieldDefinition*) node1->value;
		if(field->type==FIELD_TYPE_STRING){
			uint32 length = htonl(value->length);
			memcpy(buffer + len, &length, 4);
		}
		len += 4;
		memcpy(buffer+len, value->array, value->length);
		len+=value->length;

		node = node->next;
		node1 = node1->next;
	}
	return result;
}

int insertRecord(SimpleDatabase *database, const char *databasename, const char *tablename, List *values){
	//TODO 内存泄露
	HashMap *tableMap = (HashMap*)getHashMap(database->databaseMap, strlen(databasename), (uint8*)databasename);
	if(tableMap==NULL){
		printf("数据库 %s 不存在, 请先创建数据库\n", databasename);
		return 0;
	}
	List *fields = (List *)getHashMap(tableMap, strlen(tablename), (uint8 *)tablename);
	if (tableMap == NULL){
		printf("表 %s 不存在, 请先创建表\n", tablename);
		return 0;
	}
	// List<Array{length, bytes}>
	List* dumpvalues = checkAndDumpValues(fields, values);
	// 查找主键
	ListNode *node = dumpvalues->head;
	ListNode *node1 = fields->head;
	FieldDefinition *primaryKeyField = NULL;
	void* primaryKeyValue = NULL;
	int len = 0;
	while (node != NULL) {
		Array *value = (Array *)node->value;
		FieldDefinition *field = (FieldDefinition *)node1->value;

		if(field->flag==FIELD_FLAG_PRIMARY_KEY && primaryKeyField==NULL){
			primaryKeyField = field;
			char* valueWith0 = calloc(1, primaryKeyField->length); // 创建一个拷贝不足的补零, 用于索引存储
			memcpy(valueWith0, value->array, value->length);
			primaryKeyValue = valueWith0;
			break;
		}
		node = node->next;
		node1 = node1->next;
	}
	// 插入索引并计算总长度
	node = dumpvalues->head;
	node1 = fields->head;
	while (node != NULL) {
		Array *value = (Array *)node->value;
		FieldDefinition *field = (FieldDefinition *)node1->value;
		//计算长度
		len += value->length;
		if(field->type == FIELD_TYPE_STRING){
			len += 4; //字符串起始length字段
		}
		//如果是索引, 写入索引文件
		if(field->flag!=FIELD_FLAG_NORMAL){
			char *indexFilename = genIndexfilename(databasename, tablename, field->name);
			IndexEngine *indexEngine = (IndexEngine *) getHashMap(database->indexMap, strlen(indexFilename), (uint8*)indexFilename);
			if (indexEngine==NULL){
				printf("索引文件本应该存在, 但是缺失");
				return 0;
			}
			char *keyWith0 = calloc(1, field->length); // 创建一个拷贝不足的补零, 用于索引存储, 主要防止字符串问题
			memcpy(keyWith0, value->array, value->length);
			insertIndexEngine(indexEngine, (uint8*)keyWith0,(uint8*) primaryKeyValue);
			free(keyWith0);
		}
		node = node->next;
		node1 = node1->next;
	}
	// 将数据 写入
	Array * buffer = dumpvaluesToBuffer(len, fields, dumpvalues);
	char *dataFilename = genTablefilename(databasename, tablename);
	HashEngine *hashEngine = (HashEngine *)getHashMap(database->dataMap, strlen(dataFilename), (uint8*)dataFilename);
	putHashEngine(hashEngine, strlen(primaryKeyValue), primaryKeyValue, buffer->length, buffer->array);
	return 1;
}