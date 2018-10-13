/**
 * Copyright (c) 2018, rectcircle. All rights reserved.
 *
 * @file test-test.cpp 
 * @author rectcircle 
 * @date 2018-10-06 
 * @version 0.0.1
 */
#include "test.h"
#include "util.h"
#include "indexengine.h"

void debug(){
	printf("%d %d\n", 1, htonl(1));
	printf("%lld %lld\n", 1ll, htonll(1ll));
}

void testReadWriteMeta(){
	char* filename = "test.idx";
	//od -t x1 test.idx
	IndexEngine *engine1 = makeIndexEngine(filename, 1, 1, 0, 0, 0);
	close(engine1->fd);
	IndexEngine *engine2 = loadIndexEngine(filename, 0);
	printf("count=%lld %lld\n", engine1->count, engine2->count);
	assertlonglong(engine1->count, engine2->count, "count");
	printf("filename=%s %s\n", engine1->filename, engine2->filename);
	assertstring(engine1->filename, engine2->filename, "filename");
	printf("flag=%d %d\n", engine1->flag, engine2->flag);
	assertint(engine1->flag, engine2->flag, "flag");
	printf("magic=%x %x\n", engine1->magic, engine2->magic);
	assertint(engine1->magic, engine2->magic, "magic");
	printf("nextPageId=%lld %lld\n", engine1->nextPageId, engine2->nextPageId);
	assertlonglong(engine1->nextPageId, engine2->nextPageId, "nextPageId");
	printf("pageSize=%d %d\n", engine1->pageSize, engine2->pageSize);
	assertint(engine1->pageSize, engine2->pageSize, "pageSize");
	printf("usedPageCnt=%lld %lld\n", engine1->usedPageCnt, engine2->usedPageCnt);
	assertlonglong(engine1->usedPageCnt, engine2->usedPageCnt, "usedPageCnt");
	printf("version=%d %d\n", engine1->version, engine2->version);
	assertint(engine1->version, engine2->version, "version");
	printf("degree=%d %d\n", engine1->treeMeta.degree, engine2->treeMeta.degree);
	assertint(engine1->treeMeta.degree, engine2->treeMeta.degree, "treeMeta.degree");
	printf("depth=%d %d\n", engine1->treeMeta.depth, engine2->treeMeta.depth);
	assertint(engine1->treeMeta.depth, engine2->treeMeta.depth, "treeMeta.depth");
	printf("isUnique=%d %d\n", (int)engine1->treeMeta.isUnique, (int)engine2->treeMeta.isUnique);
	assertbool(engine1->treeMeta.isUnique, engine2->treeMeta.isUnique, "treeMeta.isUnique");
	printf("keyLen=%d %d\n", engine1->treeMeta.keyLen, engine2->treeMeta.keyLen);
	assertint(engine1->treeMeta.keyLen, engine2->treeMeta.keyLen, "treeMeta.keyLen");
	printf("valueLen=%d %d\n", engine1->treeMeta.valueLen, engine2->treeMeta.valueLen);
	assertint(engine1->treeMeta.valueLen, engine2->treeMeta.valueLen, "treeMeta.valueLen");
	printf("root=%lld %lld\n", engine1->treeMeta.root, engine2->treeMeta.root);
	assertlonglong(engine1->treeMeta.root, engine2->treeMeta.root, "treeMeta.root");
	printf("sqt=%lld %lld\n", engine1->treeMeta.sqt, engine2->treeMeta.sqt);
	assertlonglong(engine1->treeMeta.sqt, engine2->treeMeta.sqt, "treeMeta.sqt");
	printf("nextNodeVersion=%lld %lld\n", engine1->nextNodeVersion, engine2->nextNodeVersion);
	assertlonglong(engine1->nextNodeVersion, engine2->nextNodeVersion, "nextNodeVersion");
	unlink(filename);
}

TESTFUNC funcs[] = {
	debug, 
	testReadWriteMeta
};

int main(int argc, char const *argv[])
{
	launchTestArray(sizeof(funcs)/sizeof(TESTFUNC), funcs);
	return 0;
}


