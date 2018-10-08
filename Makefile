DIR_INC = ./include
DIR_SRC = ./src
DIR_SRC_MAIN = ${DIR_SRC}/main
DIR_SRC_TEST = ${DIR_SRC}/test
DIR_OUT = ./out
# 发行输出目录
DIR_OBJ = ${DIR_OUT}/obj/release
DIR_BIN = ${DIR_OUT}/bin
# debug输出目录
DIR_OBJ_BEBUG = ${DIR_OUT}/obj/debug
DIR_BIN_BEBUG = ${DIR_OUT}/test

# src 组件名（文件名，不包含后缀）
SRC = $(wildcard ${DIR_SRC}/*.c)
OBJ = $(patsubst %.c,${DIR_OBJ}/%.o,$(notdir ${SRC})) 
OBJ_DEBUG = $(patsubst %.c,${DIR_OBJ_BEBUG}/%.o,$(notdir ${SRC})) 

# 编译器配置
CC = gcc
CFLAGS = -std=c99 -Wall -I${DIR_INC}
CFLAGS_DEBUG = -std=c99 -DPROFILE_TEST -g -Wall -I${DIR_INC}

#要构建的主程序名
TARGET = $(patsubst %.c,%,$(notdir $(wildcard ${DIR_SRC_MAIN}/*.c)))
# 要构建的测试程序名
TARGET_TEST =  $(patsubst %.c,%,$(notdir $(wildcard ${DIR_SRC_TEST}/*.c)))

# 编译所有主程序
main:${TARGET}
	@echo "\e[1;32m构建完成\e[0m"

mkoutdir:
	-mkdir -p ${DIR_OUT}
	-mkdir -p ${DIR_OBJ}
	-mkdir -p ${DIR_BIN}
	-mkdir -p ${DIR_OBJ_BEBUG}
	-mkdir -p ${DIR_BIN_BEBUG}

#编译所有测试程序
test:${TARGET_TEST}
	@echo "\e[1;32m测试程序构建完成\e[0m"

#运行所有测试程序
runtest: $(patsubst %,run-%,${TARGET_TEST})
	@echo "\e[1;32m所有测试程序运行完成\e[0m"

#运行某个测试程序
run-%: %
	@echo "\e[1;32m开始运行测试程序$(patsubst run-%,%,$@)\e[0m"
	${DIR_BIN_BEBUG}/$(patsubst run-%,%,$@)

#测试配置用
# echo:
# 	echo ${TARGET_TEST}

#编译发行程序
main-%:${OBJ} ${DIR_OBJ}/main-%.o
	$(CC) $(OBJ) ${DIR_OBJ}/$@.o -o ${DIR_BIN}/$@

#测试程序，依赖于组件，输出到test
test-%:${OBJ_DEBUG} ${DIR_OBJ_BEBUG}/test-%.o
	$(CC) $(OBJ_DEBUG) ${DIR_OBJ_BEBUG}/$@.o -o ${DIR_BIN_BEBUG}/$@

# 编译src组件代码，不使用使用-g参数
${DIR_OBJ}/%.o:${DIR_SRC}/%.c
	$(CC) $(CFLAGS) -c  $< -o $@
# 编译src组件代码，使用-g参数
${DIR_OBJ_BEBUG}/%.o:${DIR_SRC}/%.c
	$(CC) $(CFLAGS_DEBUG) -c  $< -o $@


# 编译src主程序代码，包含main程序，不使用-g
${DIR_OBJ}/%.o:${DIR_SRC_MAIN}/%.c
	$(CC) $(CFLAGS) -c  $< -o $@
# 编译test代码，包含main程序，使用-g
${DIR_OBJ_BEBUG}/%.o:${DIR_SRC_TEST}/%.c
	$(CC) $(CFLAGS_DEBUG) -c  $< -o $@
.PHONY:clean
clean:
	find ${DIR_OUT} -type f -exec rm -rf {} \;