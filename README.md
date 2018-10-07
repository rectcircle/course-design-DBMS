# DBMS设计实现

## 任务记录

* [x] 内存上实现B+树
  * [x] 2018-09-25 实现B+树的查找
  * [x] 2018-10-01 实现B+树的插入
  * [x] 2018-10-01 优化，去除B+树插入过程中的多余的一次二分查找
  * [x] 2018-10-02 重构数据结构，采用key数目与指针数目相同的设计
    * 使用层级判断是否是叶子节点
    * 重构数据结构
    * 重构makeBTreeNode
    * 重构makeBTree
    * 重构binarySearch
    * 重构insert
    * 重构search
  * [x] 2018-10-03 实现B+树的删除
  * [x] 2018-10-03 重构代码
    * 考虑注释规范
    * 将工具相关代码移到util.c下
    * 将测试代码移到btree-test.c下
  * [x] 2018-10-03 启用BTreeeisUnique字段
  * [x] 2018-10-03 树中存在一个key对应多个value，批量删除
  * [x] 2018-10-03 删除函数添加value参数（可选），用于选中kv一对多情况
  * [x] 2018-10-03 修改查询返回二级指针——以应对kv一对多问题
  * [x] 2018-10-03 添加update函数
  * [x] 2018-10-03 考虑是否使用有类型的KEY：不需要，交由上层处理
  * [x] 2018-10-03 修复小端模式下异常（统一使用大端模式储存、通过宏或hton）：交由上层处理
  * [x] 2018-10-04 完善文档、图示等内容
* [x] 2018-10-05 编写Makefile
* [x] 2018-10-05 优化项目目录结构
* [x] 2018-10-06 实现一个通用LRU缓存
  * [x] 实现HashMap
  * [x] 实现双向链表
  * [x] 实现LRU
  * [x] 添加淘汰钩子
* [x] 2018-10-06 实现一个通用测试框架
* [ ] 修正LRU设计缺陷：value类型使用`void*`
* [ ] 结合LRU和内存B+树实现磁盘B+树设计存储引擎
  * [ ] 2018-10-07 概要设计
    * 大小端问题
    * 有符号无符号整形类型存储问题
    * 故障恢复
    * 持久化
    * 碎片整理
* [ ] 存储引擎实现提交日志相关设计

## 目录结构

```tree
.
├── doc
├── include
├── LICENSE
├── Makefile
├── make.sh
├── out
│   ├── bin
│   ├── obj
│   │   ├── debug
│   │   └── release
│   └── test
├── README.md
└── src
    ├── main
    └── test
```

* doc 文档
* include 头文件
* LICENSE 源码许可证
* Makefile Makefile文件
* ~~make.sh 编译脚本，已废弃，请忽略~~
* out 编译输出目录
  * bin 可执行文件目录
  * obj 目标对象目录
    * debug 包含调试信息的目标对象
    * release 不包含调试信息的目标对象
  * test 测试的可执行文件
* README.md
* src 源文件目录，该目录下的文件不包含main函数
  * main 包含main函数的c程序，命名以`main-`开头
  * test 包含main函数的测试源文件，命名以`test-`开头

### make

* `make mkoutdir` 创建输出目录，如果以下命令执行失败请执行该命令
* `make` 编译生成可执行文件
* `make main` 编译生成可执行文件
* `make test` 编译生成可执行的测试文件
* `make runtest` 编译运行所有测试文件
* `make run-%` 编译运行某测试文件
* `make main-%` 编译某可执行程序
* `make test-%` 编译某可执行测试程序
* `make clean` 清理make输出目录