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
	SimpleDatabase* dbms = malloc(sizeof(SimpleDatabase));
	newAndCopyByteArray((uint8**)&dbms->dirpath, (uint8*)dirpath, strlen(dirpath)+1);
	int hashMapSize = 1024;
	int metadataHashMapCap = 1024;
	int metadataCacheCap = 1024;
	dbms->databaseMap = makeHashMap(hashMapSize);
	dbms->dataMap = makeHashMap(hashMapSize);
	dbms->indexMap = makeHashMap(hashMapSize);
	char *metadataPath = genMetadatapath(dirpath);
	HashEngine* metadateHashEngine = makeHashEngine(metadataPath, metadataCacheCap, metadataHashMapCap, 1024, sizeThreshold, 1024);
	putHashMap(dbms->dataMap, strlen(METADATA_KEY), (uint8*)METADATA_KEY,metadateHashEngine);

	free(metadataPath);
	return dbms;
}

int createDatabase(SimpleDatabase *dbms, const char *databasename){
	int hashMapCap = 1024;
	if(getHashMap(dbms->databaseMap, strlen(databasename), (uint8*) databasename)!=NULL){
		printf("数据库 %s 已存在", databasename);
		return 0;
	}
	HashMap *tableMap = makeHashMap(hashMapCap);
	putHashMap(dbms->databaseMap, strlen(databasename), (uint8*)databasename, tableMap);
	return 1;
}

int createTable(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *fields){
	int dataHashMapCap = 1024;
	int dataCacheCap = 1024;
	HashMap *tableMap = (HashMap*)getHashMap(dbms->databaseMap, strlen(databasename), (uint8*)databasename);
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
	char * tableFilepath = genFullpath(dbms->dirpath, tableFilename);
	//创建HashEngine
	HashEngine *tableData = makeHashEngine(tableFilepath, dataHashMapCap, dataCacheCap, 1024, sizeThreshold, 1024);
	putHashMap(dbms->dataMap, strlen(tableFilename), (uint8 *)tableFilename, tableData);
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
		char *indexFilepath = genFullpath(dbms->dirpath, indexFilename);
		// 创建索引引擎
		IndexEngine *tableIndex = makeIndexEngine(
			indexFilepath,
			field->length,
			primaryKeyField->length,
			pageSize,
			isUnique,
			maxHeapSize,
			1024, sizeThreshold, 1024);
		putHashMap(dbms->indexMap, strlen(indexFilename), (uint8 *)indexFilename, tableIndex);
		free(indexFilename);
		free(indexFilepath);
		node = node->next;
	}
	free(tableFilename);
	free(tableFilepath);
	return 1;
}

/**
 * 获取表定义
 */
List *getFieldDefinitions(SimpleDatabase *dbms, const char *databasename, const char *tablename){
	HashMap *tableMap = (HashMap*)getHashMap(dbms->databaseMap, strlen(databasename), (uint8*)databasename);
	if(tableMap==NULL){
		printf("数据库 %s 不存在, 请先创建数据库\n", databasename);
		return NULL;
	}
	List *fields = (List *)getHashMap(tableMap, strlen(tablename), (uint8 *)tablename);
	if (fields == NULL){
		printf("表 %s 不存在, 请先创建表\n", tablename);
		return NULL;
	}
	return fields;
}

FieldDefinition* getPrimaryKey(List* fields){
	ListNode* node = fields->head;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition *)node->value;
		if(field->flag == FIELD_FLAG_PRIMARY_KEY){
			return field;
		}
		node = node->next;
	}
	return NULL;
}

FieldDefinition* getFieldByName(List* fields, const char* name){
	ListNode* node = fields->head;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition *)node->value;
		if(strcmp(field->name, name)==0){
			return field;
		}
		node = node->next;
	}
	return NULL;
}

static Array* dumpValue(FieldDefinition *field, void *value){
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
	return data;
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
		Array* data = dumpValue(field, value);
		addList(result, data);
		node1 = node1->next;
		node2 = node2->next;
	}
	return result;
}

static Array* dumpvaluesToBuffer(int32 bufferLen, List* fields, List*dumpvalues){
	uint8 *buffer = malloc(bufferLen);
	Array* result = malloc(sizeof(Array));
	result->length = bufferLen;
	result->array = buffer;
	uint32 len = 0;
	ListNode* node = dumpvalues->head;
	ListNode *node1 = fields->head;
	while (node != NULL) {
		Array* value = (Array*) node->value;
		FieldDefinition* field = (FieldDefinition*) node1->value;
		if(field->type==FIELD_TYPE_STRING){
			uint32 length = htonl(value->length);
			memcpy(buffer + len, &length, 4);
			len += 4;
		}
		memcpy(buffer+len, value->array, value->length);
		len+=value->length;

		node = node->next;
		node1 = node1->next;
	}
	return result;
}

int insertRecord(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *values){
	//TODO 内存泄露
	List *fields = getFieldDefinitions(dbms, databasename, tablename);
	if (fields == NULL){
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
			IndexEngine *indexEngine = (IndexEngine *) getHashMap(dbms->indexMap, strlen(indexFilename), (uint8*)indexFilename);
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
	HashEngine *hashEngine = (HashEngine *)getHashMap(dbms->dataMap, strlen(dataFilename), (uint8*)dataFilename);
	putHashEngine(hashEngine, primaryKeyField->length, primaryKeyValue, buffer->length, buffer->array);
	return 1;
}

// Array* -> <List<void*>
static List* parseRecord(List* fields, Array* hashResult){
	List* result = makeList();
	ListNode *node = fields->head;
	uint32 len = 0;
	uint8* values = (uint8*)hashResult->array;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition *)node->value;
		if(field->type==FIELD_TYPE_STRING){
			//字符串类型
			uint32 strLen = *(uint32*) (values+len);
			len += 4;
			strLen = ntohl(strLen);
			char* value = calloc(1, strLen+1);
			memcpy(value, values+len, strLen);
			len += strLen;
			addList(result, value);
		} else {
			if(field->length==1){
				uint8 originNumber = *(uint8 *)(values + len);
				uint8 *number = malloc(field->length);
				*number = originNumber;
				addList(result, number);
			} else if (field->length==2){
				uint16 originNumber = *(uint16 *)(values + len);
				uint16 *number = malloc(field->length);
				*number = ntohs(originNumber);
				addList(result, number);
			} else if(field->length==4){
				uint32 originNumber = *(uint32 *)(values + len);
				uint32 *number = malloc(field->length);
				*number = ntohl(originNumber);
				addList(result, number);
			} else if(field->length==8){
				uint64 originNumber = *(uint64 *)(values + len);
				uint64 *number = malloc(field->length);
				*number = htonll(originNumber);
				addList(result, number);
			}
			len += field->length;
		}
		node = node->next;
	}
	return result;
}

static int comparePrimaryKey(void *a, void *b, void *args){
	FieldDefinition *primaryKeyField = (FieldDefinition *)args;
	return byteArrayCompare(primaryKeyField->length, a, b);
}

/**
 * 解析条件, 并查询索引
 * @return List<void* 主键> 
 * 	   NULL 表示查询全部
 *     len() == 0 表示没有查询集为NULL;
 */
static List* parseConditions(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *conditions, FieldDefinition *primaryKeyField ){
	//内存泄露
	List *fields = getFieldDefinitions(dbms, databasename, tablename);
	List *result = makeList();
	ListNode *node = conditions->head;
	while (node != NULL) {
		QueryCondition *cond = (QueryCondition *)node->value;
		ListNode *node1 = fields->head;
		FieldDefinition* targetField = NULL;
		while (node1 != NULL) {
			FieldDefinition *field = (FieldDefinition *)node1->value;
			if(strcmp(cond->name, field->name)==0){
				targetField = field;
				break;
			}
			node1 = node1->next;
		}
		if(targetField->flag == FIELD_FLAG_NORMAL){
			freeList(result);
			return NULL;
		} else {
			char* indexfilename = genIndexfilename(databasename, tablename, targetField->name);
			IndexEngine* indexEngine = (IndexEngine*) getHashMap(dbms->indexMap, strlen(indexfilename), (uint8*)indexfilename);
			Array* key = dumpValue(targetField, cond->value);
			List *now = searchConditionIndexEngine(indexEngine, key->array, cond->relOp);
			addListToList(result, now);
		}
		node = node->next;
	}
	distinctList(result, comparePrimaryKey, primaryKeyField);
	return result;
}

static List *queryHashEngineByPrimaryList(HashEngine* engine, FieldDefinition *primaryKeyField, List *primaryKeyList){
	List* result = makeList();
	ListNode *node = primaryKeyList->head;
	while (node != NULL) {
		void* key = node->value;
		Array value = getHashEngine(engine, primaryKeyField->length, key);
		Array* array = malloc(sizeof(Array));
		array->length = value.length;
		newAndCopyByteArray((uint8**)&array->array, (uint8*)value.array, array->length);
		addList(result, array);
		node = node->next;
	}
	return result;
}

static int conditionTest(void* a, void* b, QueryCondition* cond ,FieldDefinition* field){
	int result = 0;
	if (field->type == FIELD_TYPE_STRING){
		result = strcmp((char *)a, (char *)b);
	} else if(field->type == FIELD_TYPE_UINT){
		if(field->length==1){
			result = (*((uint8*) a)) - (*((uint8*) b));
		} else if(field->length==2){
			result = (*((uint16*) a)) - (*((uint16*) b));
		} else if(field->length==4){
			result = (*((uint32*) a)) - (*((uint32*) b));
		} else if(field->length==8){
			result = (*((uint64*) a)) - (*((uint64*) b));
		}
	} else if(field->type == FIELD_TYPE_INT){
		if(field->length==1){
			result = (*((int8*) a)) - (*((int8*) b));
		} else if(field->length==2){
			result = (*((int16*) a)) - (*((int16*) b));
		} else if(field->length==4){
			result = (*((int32*) a)) - (*((int32*) b));
		} else if(field->length==8){
			result = (*((int64*) a)) - (*((int64*) b));
		}
	}
	if(cond->relOp == RELOP_EQ){
		return result == 0;
	} else if (cond->relOp == RELOP_NEQ){
		return result != 0;
	} else if (cond->relOp == RELOP_LT){
		return result < 0;
	} else if (cond->relOp == RELOP_LTE){
		return result <= 0;
	} else if (cond->relOp == RELOP_GT){
		return result >= 0;
	} else if (cond->relOp == RELOP_GTE){
		return result != 0;
	} else {
		return 0;
	}
}

static void* getRecordValueByName(List* record, List* fields, const char* name){
	ListNode *node = fields->head;
	ListNode *node1 = record->head;
	while (node != NULL) {
		FieldDefinition *field = (FieldDefinition *)node->value;
		if(strcmp(field->name, name)==0){
			return node1->value;
		}
		node = node->next;
		node1 = node1->next;
	}
	return NULL;
}

static List* filterResult(List* originResult, List *conditions, List* fields){
	//TODO 内存泄露
	if(conditions==NULL){
		return originResult;
	}
	List* result = makeList();
	ListNode *node = originResult->head;
	while (node != NULL) {
		List *record = (List *)node->value;
		ListNode *node1 = conditions->head;
		int flag = 1;
		while(node1 != NULL){
			QueryCondition* cond = node1->value;
			FieldDefinition* field = getFieldByName(fields, cond->name);
			void* value = getRecordValueByName(record, fields, cond->name);
			if(!conditionTest(value, cond->value, cond, field)){
				flag = 0;
				break;
			}
			node1 = node1->next;
		}
		if(flag){
			addList(result, record);
		} else {
			// free(value);
		}
		node = node->next;
	}
	return result;
}

List *searchRecord(SimpleDatabase *dbms, const char *databasename, const char *tablename, List *conditions){
	//TODO 内存泄露
	List *fields = getFieldDefinitions(dbms, databasename, tablename);
	FieldDefinition* primaryKeyField =  getPrimaryKey(fields);
	if (fields == NULL){
		return NULL;
	}
	char *dataFilename = genTablefilename(databasename, tablename);
	HashEngine *hashEngine = (HashEngine *)getHashMap(dbms->dataMap, strlen(dataFilename), (uint8 *)dataFilename);
	//从Hash引擎拿到的数据
	// List<Array*>
	List* hashRecords = NULL;
	if (conditions==NULL){
		hashRecords = getAllHashEngine(hashEngine);
	} else {
		// 解析条件查询...
		// 查询索引
		// 循环查询Hash引擎
		List *primaryKeyList = parseConditions(dbms, databasename, tablename, conditions, primaryKeyField);
		if (primaryKeyList==NULL){
			hashRecords = getAllHashEngine(hashEngine);
		} else if(primaryKeyList->length==0) {
			// 没有主键被选中
			return makeList();
		} else {
			hashRecords = queryHashEngineByPrimaryList(hashEngine, primaryKeyField, primaryKeyList);
		}
	}
	if(hashRecords==NULL){
		return NULL;
	}

	//获取数据解析 List<Array*> -> List<List<void*>>
	List* result = makeList();
	ListNode* node = hashRecords->head;
	while (node != NULL) {
		List *record = parseRecord(fields, (Array *) node->value);
		addList(result, record);
		node = node->next;
	}
	// 过滤查询结果
	return filterResult(result, conditions, fields);
}
