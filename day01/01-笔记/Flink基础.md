# Flink

## Flink阶段课程安排

Flink分为基础+项目。

基础：11次课。

项目：11次课。

时间：从现在到国庆。

## Flink基础

### 今晚的课程内容介绍

* 为什么学Flink
* 大数据**计算**框架的发展历史
* 批量计算 & 流式计算
* Flink简介
* Flink架构（简单版）
* Flink安装部署（Standalone模式）

## 为什么学Flink

* 薪资高

* 学习难度会比Spark稍大（Java）
* 和前面的技术栈关联不是很大
* 实时计算是未来的发展趋势

补充说明：结合DataStream API& SQL。

以DataStream API为辅，SQL为主来讲解Flink。

## 大数据计算框架的发展历史

Hadoop生态。

广义上：大数据的技术栈。Sqoop，HDFS，MapReduce，Hive、Spark等。

狭义上：Hadoop = HDFS（存储） + Yarn（资源管理） + MapReduce（计算）

大数据技术分类：

~~~shell
#1.采集
DataX：阿里的，全量采集
Canal：阿里的，增量采集
Kettle：原理和DataX类似，只是提供了可视化界面的方式进行采集而已
Flume：Apache的，实时日志采集
Sqoop：Apache的，离线关系型数据库和大数据平台之间的数据同步工具


#2.存储
HDFS：
ElasticSearch（ES）：
HBase：存储依赖于HDFS
Doris：
ClickHouse：
Greenplum：
Kafka：默认保存7天，非永久存储


#3.计算
Mapreduce（MR）：
Hive（MR的客户端）：严格来说不是，是一个MR的客户端
Spark：
Flink：
Tez：
Storm：

#4.调度
Azkaban：
DolphinScheduler：
Oozie：
Yarn：是一个分布式资源管理框架，能够让各种程序运行在Yarn上
~~~

截图如下：

![1691066191714](assets/1691066191714.png)

批量：mapreduce ，Hive，Tez，Spark

实时：Storm，StructuredStreaming，Flink

## 批量&流式计算

### 批量计算

特点：时间边界（开始、结束）

计算有延迟（隔多久计算一次）

计算是一批一批地计算，来一批，处理一批

计算是有边界的（**数据是有开始，也有结束**）

### 批量的优缺点

#### 优点

能一次性处理海量数据。

很多场景在批量下非常适合。

#### 缺点

延迟高（隔多久就延迟多久）

计算慢（Spark相比Hive还是可以）

场景：无法满足对一些时效性要求非常高的场景。

### 流式计算

场景：可以满足一些对时效性要求非常高的场景。

概念：数据是一条一条地计算，来一条，处理一条。

特点：

* 时效性高
* 数据是实时产生，实时达到，实时处理，实时计算，实时出结果

举例：

![1691067885189](assets/1691067885189.png)

![1691067940068](assets/1691067940068.png)

![1691067990225](assets/1691067990225.png)

![1691068028967](assets/1691068028967.png)

这些实时场景中，特点大致如下：

* 数据是来一条处理一条
* 数据是无界的（**数据是有开始，没有结束**）
* 数据是实时产生、实时处理、计算、出结果

### 流式计算和Flink的关系

流式计算，是一种计算思想。她是抽象的，不涉及某一门具体的计算框架。

凡是实现了流式计算思想的框架，我们就称之为流式计算框架。

Flink：它是实现了流式计算思想的一门框架而已。除了Flink之外，

还有其他技术栈，也实现了流式计算思想。比如：Storm、Spark的StructuredStreaming。

## Flink概述

### Flink介绍

Apache Flink：基于数据流上有状态的计算。

数据流：流动的数据。在实时计算中，数据是流动的。我们把流动的数据称之为数据流。

状态：计算的**中间**结果

有状态：Flink会帮我们保存计算的中间结果（Storm框架不会保存计算的中间结果，Storm是无状态计算）

### Flink历史

Flink在2008年起源于欧洲柏林工业大学，是该大学的研究下项目，项目名为StrateSphere

2014年4月份捐赠给了Apache，开始孵化

在2014年底，孵化成功，称为Apache顶级项目

2019年1月份，Flink被阿里巴巴收购的是Flink的母公司，Flink从此归属于阿里了

2023年，Flink的最新版为1.17，基本上维持着一个月一个版本的更新迭代。社区非常活跃。

### Flink架构（简单版）

如下图：

![1691070327721](assets/1691070327721.png)

主：集群管理，从节点管理，集群资源管理，Slot插槽管理，容错（checkpoint），调度

从：数据计算、该节点资源管理，Slot插槽划分

Slot：插槽，槽，这个是槽是Flink的资源单位，任何Flink的任务，都必须运行在Slot里面。

Slot一旦设置后，就不能更改，如果需要更改，则必须重启集群才行。

![1691070188577](assets/1691070188577.png)

### Flink的特点

* Exactly-once语义
* 事件时间处理
* 分层API（三层）
* 保存点技术（Savepoint，注意区别于checkpoint）
* 高吞吐、低延迟、内存计算
* 支持多种窗口机制、水印机制、容错策略等
* 支持背压机制
* 流批统一计算引擎

### Flink使用场景

* 事件驱动

Flink是以事件时间来驱动计算的

* 流批分析

通过一些算子（方法、函数）来进行分析处理

* 流式管道

Flink项目中体现

### Flink的生态

Flink和Spark类似于，也是有较完整的生态。

~~~shell
#1.代码
DataStream API（类似于SparkCore）
SQL（类似于SparkSQL）

#2.机器学习
Flink ML（类似于SparkMLLib）
Alink（专门用于集群学习）
#3.图计算
图计算Gelly，类似于Spark的GraphX
~~~











