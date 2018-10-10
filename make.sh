#!/bin/bash
#############
##  已废弃  ##
#############

PWD=$(pwd)
#输出目录相关
#输出目录
OUT_DIR="$PWD/out"
#编译成的目标文件存放目录
OBJECT_OUT_DIR="$OUT_DIR/obj"
OBJECT_REL_OUT_DIR="$OBJECT_OUT_DIR/release"
OBJECT_DEBUG_OUT_DIR="$OBJECT_OUT_DIR/debug"
#测试目录
TEST_OUT_DIR="$OUT_DIR/test"
#发布目录
DIST_OUT_DIR="$OUT_DIR/bin"

#数组用于快速创建目录
DIR_ARRAY=($OUT_DIR \
	$OBJECT_OUT_DIR $OBJECT_REL_OUT_DIR $OBJECT_DEBUG_OUT_DIR \
	$TEST_OUT_DIR $DIST_OUT_DIR)

#源代码目录
SRC_DIR="$PWD/src"

function echoUsage {
	echo \
"Usage:  ./make.sh clean
	./make.sh compile [-g] [module]
	./make.sh test [module]
	./make.sh dist

	clean 删除输出目录
	compile 编译整个项目
		-g 带调试信息的编译
		module 编译指定模块
	test 执行所有测试代码
		module 执行指定模块的测试文件
	dist 编译项目为可执行文件
"
}

function clean {
	rm -rf $OUT_DIR
	mkoutdir
}

function mkoutdir {
	for dir in ${DIR_ARRAY[@]}  
	do
		if [ ! -d "$dir" ]; then
			mkdir $dir
		fi
	done
}

function compile {
	mkoutdir
	CC="gcc"
	dir=$OBJECT_REL_OUT_DIR
	if [ "$1" = "-g" ]; then
		CC="$CC -g"
		dir=$OBJECT_DEBUG_OUT_DIR
	fi
	for file in `ls $SRC_DIR`
	do 
		if [ "${file##*.}" = "c" ]; then
			$CC -I$SRC_DIR -c $SRC_DIR/$file -o $dir/${file%*.c}.o
		fi
	done
}

# 去除参数的第一个参数并输出
function _arrayTail {
	i=0
	for p in $@
	do
		if [ $i -ne 0 ]; then
			param[i]=$p
		fi
		i=$[i+1]
	done
	echo ${param[@]}
}

function main {
	if [ $# -eq 0 ]; then
		echoUsage
		return
	fi
	param=$(_arrayTail $@)
	$1 $param
	if [ $? -ne 0 ]; then
		echo "Error!!!"
		echoUsage
	fi
}

main $@