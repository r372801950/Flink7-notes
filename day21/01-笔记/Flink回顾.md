# Flink回顾

## 今晚内容介绍

* Flink基础内容回顾
* Flink项目内容回顾

## Flink基础

### 为什么学Flink

* 趋势（流处理）
* 提升竞争力
* 薪资

### 大数据计算框架发展历史

#### 离线

MapReduce -> Hive（MR客户端）-> Tez（Hive的一种执行引擎）->Spark（内存迭代）

#### 实时

Storm（暴风、风暴）->StructuredStreaming（结构化）->Flink（松鼠）

除了计算框架之外，大数据领域还有其他的一些框架：

~~~shell
#1.采集
Sqoop、DataX、Canal、Kettle、Flume、FlinkCDC

#2.存储
HDFS（Hive、HBase）、Kafka、Hudi、Doris、OSS（对象存储）

#3.计算
MapReduce、Spark、Flink

#4.调度
Oozie、Azkaban、DolphinScheduler
~~~

### 流式计算

#### 批量计算

数据是一批一批计算，来一批处理一批

延迟较高

资源消耗较低

#### 场景

在一些时效性要求比价高的场景下，批量计算捉襟见肘。

* 滴滴打车
* 外卖
* 道路拥堵
* 网站实时流式分析

#### 流式计算

数据是一条一条地计算，来一条处理一条

延迟极低

资源消耗较高

#### 流式计算和Flink有什么关系

流式计算：是一种思想。

Flink：是实现了流式计算思想的一款计算框架而已。

### Flink介绍

#### 概述

Flink：是基于数据流上有状态的计算。

数据流：流动的数据。数据是源源不断产生，源源不断到达，实时处理，实时得到结果。

状态：计算的中间结果。有状态，说的是Flink会帮我们保存计算的中间结果。

#### 架构

主：JobManager，集群管理，任务调度，监控

从：TaskManager，任务执行

#### 历史

2009年在柏林工业大学实验室被创建，2014年中旬被捐赠给了Apache，2014年底，称为顶级项目。

2019年初，被Alibaba收购。最新版为1.17.1版本。

#### 模块

* DataStream：写代码
* SQL：写SQL
* Alink：机器学习
* gelly：图计算

### Flink安装部署

Flink可以安装在Local、Standalone、Yarn。

Local：本地开启一个进程，模拟Flink的主从进程进行计算

Standalone：主进程和从进程各自独立，互相配合完成计算

Yarn：Flink程序运行在Yarn的Container中

#### Standalone模式

解压，稍微配置一下即可。

使用：

~~~shell
#1.切换路径
cd $FLINK_HOME

#2.运行
bin/flink run xxxx.jar
~~~

#### Yarn模式

Flink On Yarn有三种：

* Session（会话）
* per-job模式（job分离）
* Application（应用）

##### Session模式

所有Flink程序都运行在一个Session集群中。这个集群是提前启动的。

~~~shell
#1.切换路径
cd $FLINK_HOME

#2.启动Session集群
bin/yarn-session.sh

#3.提交任务，当Session集群启动后再提交
bin/flink run xxxx.jar
~~~

##### per-job模式

每一个任务都会创建一个Flink的集群，类似于Spark的client模式。

它的使用一步到位：

~~~shell
bin/flink run -m yarn-cluster xxxx.jar
~~~

##### Application模式

每一个任务都会创建一个Flink的集群，类似于Spark的cluster模式。

它的使用一步到位：

~~~shell
bin/flink run-application -t yarn-application xxxx.jar
~~~

### 入门案例

#### Flink分层API

* SQL/Table API，最顶层，它是抽象程度最高的
* DataStream API，核心层
* StateProcess，最底层，低价API

#### Flink程序开发流程

~~~shell
#1.构建流式执行环境

#2.数据源（Source）

#3.数据处理（Transformation）

#4.数据输出（Sink）

#5.启动流式任务
~~~

#### API介绍

##### 流式环境

~~~shell
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
~~~

##### 数据源

~~~shell
#1.文件
#2.socket
#3.集合
#4.自定义
~~~

##### 数据处理

~~~shell
flatMap
map
filter
keyBy
reduce
sum
window
process
~~~

##### 数据输出

~~~shell
#1.文件
#2.socket
#3.自定义
~~~

##### 启动流式任务

~~~shell
env.execute()
~~~

#### 入门案例

* 批处理案例（DataStream）
* 流处理案例（DataStream）
    * reduce
    * sum
    * pipeline（链式编程）
    * lambda
    * lambda最终版
* Table API案例
* SQL案例

#### 提交集群运行

* 命令行提交
* WebUI提交

##### 命令行提交

使用idea打包，把jar包上传到Linux上，然后执行flink提交命令即可。

~~~shell
bin/flink run-application -t yarn-application xxxx.jar
~~~

##### WebUI提交

点击8081页面的submit，上传jar包，输入参数，点击提交即可。

### 运行时架构

![1694865708024](assets/1694865708024.png)

#### 通信框架

Flink是Akka。Spark是Netty。

#### JobManager

##### Dispatcher

分发器，够级WebUI，提交任务给调度器的。

##### JobMaster

调度器，进行任务调度

##### ResourceManager

资源管理器，专门管理集群资源

#### TaskManager

slot，槽。任务必须运行在slot中。

每一个slot，就是并行度。

slot：是静态的。是集群启动时就设置了。

并行度：动态的概念，是任务提交时动态给的参数。

#### CheckpointCoordinator

检查点协调器，是用于流式任务容错的。

#### 内存和IO管理器

负责slot的资源管理

#### 网络管理器

是负责节点与节点之间的网络管理。当数据需要跨节点通信时，需要走网络。

#### 客户端

只是负责任务的提交，提交后断开都可以。

### 任务运行流程

#### 抽象流程

和具体的模式没关系，任何任务提交，都遵循这个流程。

* 通过客户端提交
* 如果有Dispatcher，提交给Dispatcher
* Dispatcher把任务提交给调度器
* 调度器向资源管理申请资源
* 资源管理收到请求后提交资源
* 调度任务执行

#### 独立模式

- 通过客户端提交
- 如果有Dispatcher，提交给Dispatcher
- Dispatcher把任务提交给调度器
- 调度器向资源管理申请资源，资源管理器管理着一批的从节点
- 资源管理收到请求后提交资源，由于集群中有一批的从节点，因此可以直接提供资源
- 调度任务到TaskManager上执行

#### Yarn模式

- 通过客户端提交
- 如果有Dispatcher（session），提交给Dispatcher
- Dispatcher把任务提交给调度器
- 调度器向资源管理申请资源，资源管理器向Yarn的ResourceManager申请资源
- Yarn的资源管理收到请求后动态启动一些Container，这些容器中就是自愿
- 这些容器会反向注册到资源管理，于是资源管理有了资源
- 有了资源后，调度任务到TaskManager上执行

### Flink的概念

~~~shell
#1.层级关系
Flink集群 -> Flink Job（作业） -> Task（任务） -> SubTask（子任务）

#2.算子&算子链
算子：方法
算子链：窄依赖内部的多个算子串起来一起执行

#3.槽&槽共享
slot：槽，也就是资源
槽共享：一个slot槽，可以运行多个SubTask

#4.四张图
DataFlow Graph，数据流图，带没写完就有了，客户端生成
Job Graph，任务图，是客户端根据数据流图优化而来，也是客户端生成
Execution Graph：执行图，是JobManager根据任务图解析，转换而来
Job Graph：任务图，是TaskManager根据执行图解析转换而来

#5.并行度
并行度：运行同时执行的任务数。
设置有四种方式：
（1）配置文件
（2）任务提交，推荐
（3）全局环境
（4）算子层面
~~~

### 流式表环境对象StreamTableEnvironment

#### 分层API

- SQL/Table API，最顶层，它是抽象程度最高的
- DataStream API，核心层
- StateProcess，最底层，低价API

#### 功能

~~~shell
#1.创建&管理元数据库

#2.创建&管理数据库

#3.创建&管理表

#4.创建&管理视图

~~~

#### 常用API

~~~shell
#1.执行SQLexecuteSql
可以执行任意的SQL

#2.from
读取那张表

#3.SQL查询sqlQuery
只能执行select语句
~~~

#### FlinkSQL的表类型

~~~shell
#1.临时表，用的最多
只存在于某一个会话中，随着会话而销毁而销毁

#2.永久表，用的不多
需要借助于catalog来创建

#3.外部表，一般结合临时表来使用
通过制定外部的连接器来创建
connector=xxxx
~~~

### FlinkSQL客户端介绍

#### 介绍

Flink专门提供用于执行SQL的客户端。使用如下：

~~~shell
#1.切换路径
cd $FLINK_HOME

#2.执行命令
bin/sql-client.sh

#3.入门案例
select 'hello world';
~~~

#### 显示模式

FlinkSQL客户端有三种显示模式：

~~~shell
#1.table模式
默认的模式

#2.changelog模式
变更日志

#3.tableau模式
传统关系型数据库的模式
~~~

设置方式如下：

~~~shell
set sql-client.execution.result-mode=table|changelog|tableau;
~~~

#### WordCount案例

这里的WordCount案例，使用的是纯SQL来实现。

### 数据类型

FlinkSQL的数据类型可以分为两个类型：

* 原子数据类型
* 复合数据类型

#### 原子数据类型

* 整数（int、tinyint、bigint）
* 浮点数（float、double）
* 精度类型（decimal）

* 字符串类型（string）
* 布尔类型（true、false）
* 时间日期（date、time、datetime、timestamp）

#### 复合数据类型

* 数组（array）
* 集合（set）
* map（map）
* 对象（row）

### 动态表&连续查询

动态表：相对MySQL、Hive中的静态表而言的。Flink中的表都是动态变化的，因此称为动态表。

它本质上可以理解为源源不断的数据流。表和流也是等价的。

连续查询：相比MySQL、Hive而言，一个查询就是在某一个时间得出的某个结果，这个结果一般是静态的，而且也是暂时的。

但是，FlinkSQL中的查询，虽然也是只需要查询一次，但是它的结果会动态变化。会随着数据源的不断变化而变化。这个查询不会停止，它会一直运行。这就是连续查询。

### Flink的时间

Flink中的时间有三种：

* 摄入时间
* 处理时间
* 事件时间

#### 摄入时间

Flink程序读取到数据的时间，这个时间是Flink赋予的。

这个时间几乎不用。

#### 处理时间

数据被Flink程序处理的时间，一般可以使用处理完成的时间作为处理时间。这个时间也是Flink赋予的。

这个时间很少使用。

#### 事件时间

事件时间，是数据产生时所携带的时间，这个时间是数据携带的，和Flink没关系。

这个时间是用的最多的。

### Flink的窗口

#### 为什么要学窗口

窗口是把流计算转换为批计算的桥梁。本质上就是一段时间范围（区间）。

这个窗口，是流式计算中通用的思想，并不是Flink特有的。Flink也有窗口。

Flink的窗口有如下：

* 滚动
* 滑动
* 会话
* 渐进式
* 聚合

#### 滚动

概念：窗口大小 = 滚动距离。

特点：数据不重复，不丢失。

案例：SQL（tumble）、代码。

#### 滑动

概念：窗口大小  != 滑动距离。

~~~shell
滑动距离 < 窗口大小，这种会造成数据重计算，重点
滑动距离 = 窗口大小，就是滚动窗口
滑动距离 > 窗口大小，这种会造成数据丢失，一般不用
~~~

特点：数据会重复计算。

案例：SQL（hop）、代码。

#### 会话

概念：没有固定的窗口大小，但是有固定的会话间隔时间。相邻的两条数据没有超过会话间隔，则这两条数据会在同一个会话中，反之，则在不同的会话中。

会话间隔：用户自己指定。

特点：根据数据来划分不同的会话

案例：SQL（session）、代码。

#### 渐进式

概念：在一定的时间范围内，结果呈现渐进递增的趋势。

特点：线性递增

案例：SQL。

#### 聚合

和Hive的开窗函数类似。

Flink有两种类型的聚合窗口：

* 时间区间
* 行号

##### 时间区间

~~~shell
range between 起始时间  and  current now
~~~

##### 行号

~~~shell
rows between 起始行号  and  current now
~~~

### Flink的水印

#### 为什么学水印

实际场景中，出现一些数据迟到现象，没发避免。

Flink提出了水印的概念来处理一定时间内的迟到数据。

#### 水印

Watermark，水位线，可以处理一定程序内的乱序数据（迟到数据）。

#### 案例

* SQL
    * watermark为0，不会处理吃到数据
    * watermark不为0，能够处理一定程序内的迟到数据，超过时间后，数据扔热会丢失
* DataStream
    * 单调递增水印，类似于watermark为0
    * 固定延迟水印，类似于watermark不为0
    * AllowedLateness，默认为0，窗口触发时就销毁，如果设置不为0，则可以延迟窗口的销毁时间
    * SideOutput，侧输出流，可以捕获严重迟到的数据，保证数据不丢失
    * 多并行度下的水印（设置最大的空闲等待时间）

### Flink的Checkpoint

#### 为什么学Checkpoint

流式任务必须保证7&24小时不间断运行。或者说就算出了问题，也要有相应的机制能够进行恢复。

Checkpoint就是做这个事的。

#### Checkpoint概述

Checkpoint，也称为检查点机制，可以实现流式任务的容错。

#### 执行流程

* 主节点的检查点协调器定期（周期性）发送一个个的barrier（栅栏），这个栅栏会混在数据中，随着数据流一起流向Source算子
* 当算子在加载数据时，它就正常处理数据，当算子加载到barrier（栅栏），算子停下手里的工作，然后向检查点协调器进行汇报
* 当状态汇报完后，barrier就随着数据流，流向下一个算子
* 下一个算子，在加载数据时，就正常处理数据，如果加载到barrier（栅栏），停下手里的工作，向检查点协调器进行远端汇报
* 以此类推
* 当所有算子都汇报完后，这一轮的Checkpoint就做完了

#### 重启策略

可以对失败的任务进行自动重启的。重启策略有：

* 不重启，不用
* 固定延迟，推荐使用
* 失败率重启，推荐使用
* 指数延迟，不推荐

#### 状态后端

检查点协调器存储状态的位置。

一般有三种：

* 内存，不用，默认的
* 文件系统，推荐，常用，一般存储在分布式文件系统
* RocksDB，看业务场景

容错 = Checkpoint + 重启策略 + 状态后端。

### FlinkSQL

#### 建表

~~~shell
#1.with参数
with(key=value)

#2.连接器
socket，读取socket数据源
filesystem：文件系统，读取文件
kafka/upsert-kafka：读取Kafka
print：把数据打印到标准输出
datagen：模拟数据源，源源不断地产生数据
jdbc：以jdbc的方式读取其他关系型数据库
mysql-cdc：以流式的方式读取MySQL数据源
hudi：读写hudi的数据
doris：写数据到Doris中

#3.数据格式
压缩：orc、parquet
非压缩：csv、json

#4.水印
watermark 事件时间列 as 策略表达式
~~~

#### 通用的SQL使用

略。

#### Join

Flink的join常用的有如下：

* regular join，常规join
* interval join，时间区间join
* Lookup join，维表join

##### regualr join

和普通的SQLjoin一样。分为inner、left、right、full四种。

##### interval join

在普通的SQLjoin上，加上了时间区间的条件。在一定的时间范围内，看看是否能满足要求。

也分为了：inner、left、right、full四种。

##### Lookup join

维度表数据在MySQL中，读取维表数据和业务数据进行join。

建议：如果数据量不大，可以放redis中。

如果数据量很大，可以放在HBase中。

#### 执行计划

explain + SQL语句。

整体上从上往下看，每一块（AST）都是从下往上看。

#### 集合操作

##### union并集

union：并集，会去重。

union all：并集，不去重。

##### intersect交集

intersect：交集，会去重。

intersect all：交集，不去重。

##### except差集

except：差集，会去重。

except all：差集，不去重。

#### use&show

~~~shell
#1.查看元数据库
show catalogs;

#2.切换元数据库
use catalog 元数据库的名称
~~~

### Flink的UDF

Flink的UDF函数支持四种类型：

* 一进一出，Scalar Function，类似于拼接函数
* 一进多出，Table Function，类似于爆炸函数
* 多进一出，Aggregate Function，类似于聚合函数
* 多进多出，Table Aggregate Function，类似于表值聚合函数

#### 案例

每一种函数都有案例。而且代码有些难度。尤其是聚合函数和表值聚合函数。

### FlinkSQL优化

#### 运行时参数

开启微批、状态有效期等配置。

#### 优化器参数

FlinkSQL的优化器。两阶段聚合、分桶等。

#### 表参数

基本上都是已经配置好了。比如方言、执行器等。

#### 常用的优化

* 开启微批
* 两阶段聚合
* 分桶
* filter子句（不常用）

## Flink项目

### 项目说明

~~~shell
#1.名字
博学谷大数据平台项目

#2.环境
已经都配置好了，直接使用即可
用户名：root
密码：123456

#3.项目中的核心技术点
FlinkCDC（DataX、Canal）
数据湖hudi（Delta lake，Iceberg）
doris（Clickhouse、druid、kylin）
dinky
metabase
~~~

### 背景

博学谷背景，属于教育行业。在线职业教育品牌。

### 技术选型

#### FlinkCDC

* exactly once
* SQL
* DataStream API

我们选择是2.2版本，也就是2.0之后的版本，2.0之后的版本：

* 无锁读取
* 并发读取
* 断点续传

#### Hudi

* 支持changelog模式
* 有事务
* 支持行级别的更新/删除

#### Doris

* 支持SQL标准，兼容MySQL协议
* 大数据组件的通用特性（高吞吐、低延迟、高性能）

* 极简运维

#### Flink

* 大数据唯一一款集高吞吐、低延迟、高性能于一身的流式计算框架
* 支持窗口、水印、容错等机制
* 支持SQL
* 背压等

### 非功能描述

#### 人员规模

略

#### 项目周期

略

#### 开发流程

略

#### 版本

组件基本上都是2022年下半年的组件。

~~~shell
#1.Flink版本
1.14.5

#2.hudi版本
0.11.1

#3.doris版本
1.1.0

#4.FlinkCDC版本
2.2.1
~~~

### FlinkSQL集合Hive

#### 版本兼容

1.14.5，兼容的Hive版本最高到3.1.2。最低是1.0版本。

#### 集成方式

* 使用官网的jar包，推荐
* 自己分开添加jar包，不推荐，除非第一种方式无法满足需求时

#### 案例

思路：

* 在Hive创建库、表
* 在FlinkSQL往Hive的表中插入数据
* 在Hive查看数据

### FlinkCDC

#### CDC

CDC：变更数据捕获，但凡能够捕获变更数据，都称之为CDC。狭义上的CDC值的是专门针对数据库的变更。

CDC是一种思想。它可以有多重实现。FlinkCDC只是实现了CDC这种思想的一门框架而已。

常用的CDC有哪些：

* FlinkCDC
* Sqoop
* DataX
* Canal
* Kettle

#### FlinkCDC

它是CDC的一种实现。它可以实时捕获数据库的变更数据，并且把数据同步到下游。比如：Hudi中。

#### FlinkCDC历史

2020年，发布了1.0版本。

2021年发布了2.0版本。

2023年9月，最新版为2.4版本。

项目中用的2.2版本。

#### 版本兼容

FlinkCDC和Flink的版本有匹配关系。这个兼容表在FlinkCDC的官网上。

#### connector的使用

FlinkCDC的connector，有很多。是根据不同的数据库来进行区别的。比如：

MySQL：mysql-cdc

Oracle：oracle-cdc

OceanBase：oceanbase-cdc

**命名规则：数据库名-cdc。**

#### 案例

核心：指定connector=mysql-cdc即可。

#### 消费数据模式

常用的有两种：

* initial，初始化消费，能够消费历史数据+增量数据
* latest-offset：从最近的时间开始消费，只能够消费增量数据，没办法消费历史数据

### Hudi

#### 数据湖

##### 什么是数据湖

对比生活中的水湖（不是水壶）。

满足如下2个条件就是数据湖：

* 允许海量原始数据存储
* 允许多种计算框架计算

数据湖本质上是一种存储思想。是一种先存后用的思想。

目前市场上有如下的数据湖框架，实现了这种思想的：

* Delta Lake
* Iceberg
* Hudi

#### Hudi

##### 概述

Hudi：Hadoop Upsert Delete And Incremental。

是一种管理位于分布式文件系统的数据湖框架。

##### 历史

* 2015年由Uber公司提出的论文
* 2016年开始研发
* 2017年开源
* 2019年捐赠给了Apache
* 2020年孵化成功，称为顶级项目

##### 架构

和其他数据湖框架类似。

##### 应用

大公司都在用。

#### 湖仓一体架构

为什么会有湖仓一体？

数据湖或者数据仓库，无论是哪一个技术栈都有缺陷，能不能把两个技术栈整合起来呢？

可以。

就是使用Hive数仓来管理数据湖的数据。进而能够简化公司的项目架构，统一数据源头。

#### Hudi入门案例

参考官网。

### Hudi核心理论

#### Hudi是数据管理

##### Instant

Instant，瞬态视图，它表示对Hudi表的一个操作。这个操作由三部分组成：

* 时间戳
* Action（操作）
    * COMMIT（提交）、DELTA_COMMIT（提交）、COMPACTION（压缩）、CLEAN（清理）
* State（状态）
    * REQUESTED（请求发起）
    * INFLIGHT（请求执行中）
    * COMPLTED（执行完成）

##### 索引

* Bloom Index（布隆索引，默认的）

* HBase Index（HBase索引）
* Simple Index（简单索引）
* Custom Index（自定义索引）

##### 文件存储

层次结构：Hudi表（路径） -> partition（分区） -> FileGroup（文件组）->Slice（切片）

物理层面：

* parquet：base file，基础文件
* log：avro file，日志文件

#### 读取数据方式

##### Snapshot

快照查询，Snapshot Query = Query（log + parquet）

##### Incremental

增量查询，Incremental Query = Query（log）

##### Read Optimized

读优化查询，Read Optimized = Query（parquet）

#### Hudi的表类型

##### COW

COW，CopyOnWrite，写时复制，数据在写入时，先把源表copy一份，在此基础上，再写入新的数据。所以每一次写入后都是最新的完整数据。

适合：读多写少的场景。

##### MOR

MOR，MergeOnRead，读时合并，数据写入时不做任何操作，读取的时候，把log文件和parquet文件进行合并，形成最终的文件，再读取。

适合：写多读少的场景。

### 综合案例

把数据从MySQL同步到Hudi的案例。而且案例中使用了湖仓一体架构。

湖仓一体架构如何落地？

通过一系列配置即可。

### Doris

#### Doris的概述

##### 为什么学Doris

扩充实时计算生态。可以让用户在不了解Flink的前提下也可以使用实时计算框架。

##### Doris

Doris是一款基于MPP架构大规模、分析、可扩展的数据库。

MPP：类似于Hadoop中的分而治之的思想。

##### 特点

* 高可用
* 高吞吐
* 海量存储
* 标准SQL、兼容MySQL协议
* 极简运维

##### 架构

Doris的架构只有两种角色：

* FE：Frontend，前端节点，主要集群管理，元数据管理，SQL解析转换
* BE：backend，后端节点，只要是负责数据存储和计算

##### 使用

大厂（国内）都在用。

##### OLAP、OLTP、HTAP

OLAP：联机分析处理，对历史数据做分析，一般不支持数据的增伤改操作。一般没有事务。数据量一般比较大。

离线：Hive

实时：Doris

MOLAP：多维分析，比如Kylin、Druid都是MOLAP引擎

ROLAP：关系分析，比如Clickhouse、Doris都是ROLAP引擎

OLTP：联机事务处理，是针对业务的，是为了支撑业务而设置的，一般都有事务，支持增删改查操作，数据量相比OLAP而言比较小。

HTAP：混合事务分析处理，具备OLAP和OLTP的功能。比如TiDB就是。由国内的PingCAP公司研发的。

##### MySQL的客户端

如何使用MySQL的客户端呢？

用法可以很多种，核心是：

~~~shell
help + 关键词;
~~~

#### Doris的安装部署

由于Doris是由FE和BE组成。因此只需要安装配置FE和BE即可。

如何配置？

参考官网。

Doris的运维，虽然是极简运维，但是也需要了解。

#### Doris原理

##### 端口

8030：FE接受用户请求的端口

9030：MySQL登录后端管理端口

##### 组件

FE：前端节点 BE：后端节点

##### 架构组成

Doris = Google mesa + Apache Impala + ORC（列存）

##### 负载均衡

Doris自身实现数据的负载均衡。不需要用户操心，用户也无法干预。

#### Doris的实践

##### 建库建表

略。

##### 数据模型

* 聚合模型

相同的key列，value会做聚合操作。聚合方式有四种：

~~~shell
#1.replace
替换

#2.sum
求和

#3.max
求最大值

#4.min
求最小值
~~~

* 唯一模型

相同的key列，value会保证唯一性。如果key相同，则会使用新的value替换之前的value。也就是说，它会做replace操作。

唯一模型是聚合模型的特例。

* 冗余模型

冗余模型，运行数据冗余存储，不会对数据做任何操作，数据想怎么存就怎么存。

* 使用场景

聚合模型：一般适用于固定报表类场景

唯一模型：一般适用于保证key的唯一性场景，比如关系型数据库。

冗余模型：没有固定的场景，它很灵活，不受模型约束，只要前面的模型搞不定，都可以使用冗余模型，因为它存储的是明细数据。

##### Doris演示

分区（范围分区、列表分区）演示。

##### Doris数据导入

Doris支持多种数据导入方式：

* 本地文件
* 分布式系统文件
* Kafka数据源

##### Doris数据导出

一般导出数据到分布式文件系统。

注意：**==无论是数据的导入还是导出，都需要配合Broker进程使用==**

##### Doris数据删除

* 删除每一条（delete语法）
* 删除分区（drop partition）

#### Doris的常规操作

参数设置，join、order by、select等

#### Doris的Rollup

Doris默认建表是有顺序的 ，这个顺序就是默认的索引（组合索引），只要按照这个建表顺序查询就能匹配组合索引，查询效率比较高。

Rollup：能够调整列的顺序，以增加前缀索引的命中率。

#### Doris的物化视图

本质上就是Rollup，只是创建的语法和Rollup不一样。

Rollup或者物化视图的数据，是独立存储。

#### Doris的动态分区

动态分区是相比手动分区而言的。

Doris的动态分区有四种：

* hour
* day
* week
* month

动态分区和手动分区还可以互相转换。转换方式也比较简单，直接设置动态分区是否开启即可。

#### Doris的综合案例

MySQL -> Doris。

FlinkSQL操作Doris：通过connector来操作，这个connector=doris。

怎么使用？

参考官网。

### 博学谷业务

说明：博学谷的看板，除了课上讲的这几个之外，其余的看板都在`预习资料\Flink项目预习资料\02_资料\《博学谷数字化平台现有指标分析.docx》`目录下。

#### 数据流向看板

数据流转，演示整个项目的架构和时效性。

根据之前的架构进行开发。

#### 新媒体看板

演示新媒体短视频看板。

根据之前的架构进行开发。

#### 营收业绩看板

演示营收业绩看板。

根据之前架构来开发。

甚至包括简历的写法，都已经上传到了百度网盘中预习资料文件下。

### Dinky

#### 为什么学

传统方式不方便管理众多Flink任务。

传统方式中有特别多的DDL语句，会占用很多资源。

#### Dinky介绍

略。

这个工具在面试中不重要，可以说是公司自己自研的。不用说Dinky这个名字。

项目的名字可以自己随便给。

#### 整库入湖

整库入湖，是Dinky的亮点，也是用Dinky的核心原因之一。

它可以把一个库下的多张表（全部表）可以通过一个（少量的）CDC任务，把数据同步到ODS层中。

可以优化前期写的大量的DDL语句。并且减轻数据库的网络压力。

### 可视化

metabase，略。

可视化内容，在面试中可以说不是自己负责，说是同事负责即可。

### 优化

#### 资源配置

##### Flink的内存模型

难点，不是重点。通俗来说，内存模型，就是把内存划分成多块，每一块存储一些各自的内容。

对比理解：一套房子。一套房子，可以有多个房间，餐厅、客厅、卧室、厨房、卫生间等。

每个房间都有独立的功能，各个房间组合在一起，就形成了一个套件。

内存模型也类似。内存模型中有：JVM、metaspace、off-heap、heap等。

##### 并行度的优化

理论上怎么设置都可以，实际上可以根据公司的实际情况来设置。

如果刚开始，不知道设置多少，可以给个初始值，比如8。

##### Checkpoint的优化

* 开启本地恢复
* 设置任务取消时，保留Checkpoint路径
* 设置保留的副本数为2-3个
* 开启RocksDB状态后端（设置单独的SSD磁盘给RocksDB用）
* Checkpoint的时间建议是分钟级别

#### 反压

##### 什么是反压

下游算子消费速率低于上游算子的生产速率，当网络内存被耗尽时，就会出现反压。

##### 如何发现

通过8081页面即可，看任务的颜色和backpressure的指标值。

##### 如何处理

反压是天然存在的，一般不需要处理，只有持续不断的反压才需要处理。

反压的处理和资源相关。短时内可以通过设置任务的并行度来解决。根治还得从资源入手。比如内存。

如何不让增加内存，可以停掉一些其他的服务。

#### 数据倾斜

##### 如何发现数据倾斜

通过8081页面，点击SubTask页面可以看到不同的SubTask处理的数据量和大小。

当所有的SubTask的数据量和大小差不多时，说明数据没有倾斜，如果差别很大，说明数据有倾斜。

##### 如何处理

如果是聚合之前出现倾斜（概率不高），说明数据本身就是倾斜的，可以通过rebalance算子。

如果是聚合后出现倾斜（概率高），可以通过两阶段提交或者分桶来解决。

#### KafkaSource

##### 设置动态分区检测

将Flink的Source算子的并行度设置为Kafka topic的分区数。而且要设置动态分区检测功能。

##### 设置最大空闲等待时间

如果不设置，会造成Watermark对齐的问题。

通过设置最大空闲等待时间来处理。

#### FlinkSQL调优

##### 开启微批

默认FlinkSQL是流处理，当业务允许短时间内延迟时，可以使用微批功能。

##### 两阶段提交

手动设置。

##### 分桶

手动设置。

##### filter子句

优化case when语句。

## Hive

### 说说Hive和MySQL的区别

这个题，可以换着问：说说OLAP和OLTP的区别，说说数据库和数据仓库的区别。

MySQL：关系型数据库，支持数据的增删改查、支持事务、为业务服务，数据量较小。

Hive：数据仓库，不支持数据增伤改，不支持事务，为主题服务，对历史数据做分析，数据量较大。

### 内部表和外部表的区别

内部表：删除时，数据和元数据都会删除。

外部表：删除时，只删除元数据，数据不会删除。

工作中一般推荐使用外部表。

### 说说分区表和分桶表

分区表：分区就是分文件夹。

分通表：分桶就是分文件。

一般使用分区表。它本质上就是一种数据优化手段，能够减少数据扫描的范围。

### Hive的存储格式

textfile：文本格式

orc：压缩格式

parquet：压缩格式

sequencefile

一般选择orc即可。

### Hive的压缩方式

snappy、lzo、GZip

一般选择snappy即可。

### 默认情况下，HiveSQL是不是都会走MapReduce程序？

Hive是MapReduce的客户端，默认情况下，它底层走的是MapReduce程序。

并不是所有的HiveSQL语句都会走MapReduce程序。

比如如下一些默认不走MapReduce：

* select *（fetch）
* select 某一些字段
* select * ... limit 10

可以手动配置，参数是：

~~~shell
hive.fetch.task.conversion

#1.none
任何SQL都走MapReduce

#2.minimal
默认值，以上3种情况不走MapReduce

#3.more
除了上述3种情况外，还有更多的SQL不走MapReduce，比如having等
~~~

### 为什么有Hive

大数据计算框架，开源的第一代是MapReduce。按道理可以使用MapReduce即可，为什么用Hive呢？

如果是MapReduce，则必须写大量的Java代码。（标准写法是至少3个类）

Hive：可以简化大量编写的Java代码，取而代之的是，使用SQL来实现MapReduce。

对用户而言：写的是SQL语句

对计算而言：底层跑是MapReduce。

目的：为了减轻用户的操作。

前提：必须是结构化数据才行。

因为Hive是用表来管理数据。表就要求数据是结构化的。

load data （local） inpath ....等价于hadoop fs -put操作。

底层走的是映射。不会走MapReduce上传的。

### Hive开窗函数

有三类：

#### 排序类

* row_number：行号，自然序列，比如12345

* rank：排名函数，如果名次相同，排名一样，会出现跳跃。比如：12335

* dense_rank：排名函数，如果名次相同，排名一样，不会出现跳跃。比如：12334

#### 聚合类

类似于聚合函数，比如：sum、avg、count等。

#### 分析类

lag：上一个（同列）

lead：下一个（同列）

first_value：取第一个值（同列）

last_value：取最后一个值（同列）

### Hive的优化

参考面试宝典。建议的回答方式：

#### 建表层面

比如使用分区表、存储格式、压缩方式来说明。

#### SQL层面

在SQL编写，可以使用一些优化写法。谓词下推，更好的去重写法等。

#### 参数层面

从参数设置层面来聊Hive的优化。比如JVM重用。

### HiveSQL

刷题。更多的在工作中积累。参考SQL宝典。

## Spark

