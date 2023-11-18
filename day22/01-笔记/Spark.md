# Spark内容回顾

## 今晚内容介绍

* 复习Spark的任务执行流程
* SparkCore
* SparkSQL

* Spark面试题

## Spark任务执行流程

以Yarn的Cluster模式为例。

### Yarn执行流程

![1695296399917](assets/1695296399917.png)

### 任务的执行流程（微观）

![1695298038110](assets/1695298038110.png)

### Spark的概念

Spark的任务执行，由两部分组成：

* 集群资源
* 角色（进程）

#### 集群资源

Spark可以运行在多种模式下，比如：Local、Standalone、Yarn、memos、k8s等。

国内以Yarn模式居多。但是，最能代表Spark集群的是Standalone模式。

##### Standalone模式

主：Master

从：Worker

主从节点，无论是否有任务运行，他们都需要运行，因为这就是一个Spark的集群资源。

##### Yarn模式

主：ResourceManager

从：NodeManager

#### 角色（进程）

Spark无论运行在什么模式下，都由2个角色（进程）组成：

* Driver
* Executor

**==这2个进程是Spark程序跑起来之后才有的。如果程序结束了，这两个进程也没有了。==**

##### Standalone模式

在Standalone模式中，Driver进程就叫做Driver，Executor进程，就是Executor。

##### Yarn模式

在Yarn模式下，Driver进程，是运行在AppMaster进程中的。Executor进程，运行在Container进程中。

#### Spark其他的概念

![1695298551490](assets/1695298551490.png)

概念：

![1695298455747](assets/1695298455747.png)

~~~shell
#1.Application（DAG）
就是使用spark-submit提交给集群运行的一个程序

#2.Driver，是任务运行起来才有
Spark程序的老大

#3.Executor，是任务运行起来才有
Spark程序的小弟

#4.Job
根据action算子来划分，就是一个作业

#5.Stage（TaskSet）
根据Shuffle算子来划分，就是一个阶段，类似于MapReduce的map阶段和reduce阶段

#6.Task
每个Stage内都是由一个或者多个Task组成的，Task的数量就是并行度的数量

#7.并行度
并行度，就是Spark的分区数，就是HDFS的分片数。也就是Flink的并行度。
SparkCore：由父RDD的分区数决定，分区数就是并行度的数量。
SparkSQL：200
~~~

## RDD知识点

### RDD

RDD：Resilient Distributed DataSet，弹性分布式数据集。

弹性：数据可以在内存，也可以在磁盘

分布式：RDD底层是一个分布式的列表

数据集：RDD不存数据，存的是计算逻辑

### RDD的特点

~~~shell
#1.可分区
RDD可以有多个分区

#2.每个分区都有函数作用在上面
每一个分区，代码中写的计算函数，都会在每个分区内计算

#3.RDD是有血缘的
RDD是有父子依赖关系的

#4.（可选的）如果是KV类型的RDD，可以自定义分区规则
默认就是hash分区器

#5.（可选的）RDD有位置优先性
大数据领域中，数据和计算不一定会在相同的节点：
数据不动。
计算移动。
计算向数据移动。或者说移动数据不如移动计算。
~~~

### RDD的创建方式

~~~shell
#1.并行化一个集合
sc.paralliaze([1,2,3,4,5])

#2.读取外部数据源
sc.textFile("文件路径")
sc.wholeTextFiles("小文件的文件路径")

#3.由其他RDD转换而来
map_rdd = flat_map_rdd.map(lambda x :(x,1))
~~~

## DataFrame知识点

### DataFrame

DataFrame = RDD + Schema。

Schema：表结构。

DataFrame可以理解为是一个结构化的表。表是由数据+结构组成的。

### DataFrame的创建

~~~shell
#1.由createDataFrame创建
spark.createDataFrame(data=[],schema=[])


#2.由toDF转换
df = rdd.toDF(schema)

#3.直接读取文件形成一个df
方式一：spark.read.format("数据格式")
方式二：spark.read.数据格式()
~~~

### DataFrame的使用

~~~shell
#1.DSL
（1）rdd的同名算子
（2）SQL中的关键字算子
（3）可以使用pyspark.sql.functions包下的函数

#2.SQL
和HiveSQL一样
~~~

## 面试题

### 说说Spark的编程抽象

RDD。

### 什么是RDD呢？

RDD英文是Resilient Distributed DataSet，翻译为弹性分布式数据集。

弹性：数据可以在内存，也可以在磁盘。

分布式：RDD本质上是一个分布式的列表。

数据集：RDD是抽象的数据集。

### RDD存数据吗？

不存数据。存的是计算逻辑。

### RDD的特点有哪些

~~~shell
#1.可分区
RDD可以有多个分区

#2.每个分区都有函数作用在上面
每一个分区，代码中写的计算函数，都会在每个分区内计算

#3.RDD是有血缘的
RDD是有父子依赖关系的

#4.（可选的）如果是KV类型的RDD，可以自定义分区规则
默认就是hash分区器

#5.（可选的）RDD有位置优先性
大数据领域中，数据和计算不一定会在相同的节点：
数据不动。
计算移动。
计算向数据移动。或者说移动数据不如移动计算。
~~~

### Spark的算子类别有哪些

2类。

action（触发类、执行类）

Transformation（转换类）

### 每一个类别各举例说明

#### Transformation

map、flatMap、filter、mapPartition、coalesce、repartition、reduceByKey、sortBy、sortByKey等

#### Action

top、take、first、collect、foreach、foreachPartition、saveAsTextFile等

### repartition和coalesce区别

共同点：重分区算子。

repartition：调整分区，会Shuffle过程，一般用于调大分区，底层调用的是coalesce算子。

coalesce：调整分区，没有Shuffle过程，一般用于调小分区。

### sortBy和sortByKey区别

sortBy：排序算子，任何RDD都可以，不限制。它可以针对任何列排序。

sortByKey：排序算子，只能用于KV类型的RDD算子。只能针对key排序。

### Spark读取小文件的算子

wholeTextFiles

### Spark的任务由什么划分

Action算子

### 宽依赖可以划分什么

Stage。

### Spark的宽窄依赖

宽依赖：一个子RDD的的数据来自于多个父RDD的数据。

窄依赖：一个子RDD的数据来自于一个父RDD的数据。完全接受父RDD的数据。

### Spark的广播变量

是属于Spark的优化类算子。如果不适用广播变量，在程序进行计算的时候，每一个Task都需要去Driver端拉取数据。

会造成数据的重复拷贝过程，消耗时间。

如果使用广播变量，可以把Driver的公共数据一次性地广播到所有的Executor上，Task就不需要去Driver端拉取，而是去Executor上读取即可。能够提升计算效率。

==广播变量在Executor端是只读的，不能修改，它只能在Driver端修改。==

广播变量，它可以Driver端的数据集广播出去，请问，这个数据集是多大呢？

默认情况下，这个广播变量的数据集大小有限制，不能超过4M。参数如下：

~~~shell
spark.broadcast.blockSize
~~~

### Spark的缓存机制

#### cache

缓存，仅是把数据缓存在内存中。

底层调用的是persist，只是存储级别为StorageLevel.MEMORY_ONLY。

效率是最高的。但是数据无法永久保存，容易丢失。

#### persist

持久化，也是Spark缓存的一种级别。它可以配置不同的存储级别。比如：内存、磁盘，磁盘和内存等。

~~~shell
#1.仅内存
StorageLevel.MEMORY_ONLY

#2.仅磁盘
StorageLevel.DISK_ONLY

#3.磁盘和内存
StorageLevel.MEMORY_AND_DISK
~~~

#### Checkpoint

检查点。它可以把数据持久化到文件系统中。

开发测试可以是本地文件系统。

生产上一般是分布式文件系统。

### 这些缓存有什么区别

~~~shell
#1.数据存储位置不同
cache：内存
persist：可以在内存，也可以在磁盘
Checkpoint：分布式文件系统

#2.存储内容不同
cache和persist：数据+依赖关系
Checkpoint：只存数据，斩断依赖关系

#3.生命周期不同
cache：用完后就结束了
persist：任务结束后或者手动调用unpersist算子后释放
Checkpoint：存储在分布式文件系统，会持久化保存，不会丢失
~~~

### Spark的任务执行流程

略。以Spark的cluster模式为例。

### Spark的部署模式有哪些

部署模式和执行模式是两码事。

部署模式指的是`--deploy-mode`参数。

~~~shell
#1.client
客户端模式，Driver进程运行在客户端所在的节点上

#2.cluster
集群模式，Driver进程运行在集群中的某一个节点上
~~~

这个参数决定了Driver进程运行的位置。

### Spark的执行模式有哪些

Local：本地模式

Standalone：独立模式

Yarn：Yarn模式

mesos：

k8s：

执行模式由`--master`参数决定。

### Spark提交任务的常用参数

#### 格式

Spark的任务提交，无论是什么模式，任务参数都是如下四类：

~~~shell
#1.Master相关，以什么模式运行
--master,默认是local
--deploy-mode，默认是client

#2.Driver相关，Spark由Driver和Executor组成
--driver-memory 1g
--driver-cores 1

#3.Executor相关，Spark由Driver和Executor组成
--executor-memory 1g
--executor-cores 1
--executor的数量（不同模式下，配置参数不同）

#3.other相关，Spark任务运行执行了什么其他参数
--jars：任务运行需要的额外jar包
--conf：额外的配置
--queue：队列
--name：任务的名称
xxxx.jar（代码） | xxxx.py（Python） | xxxx.zip
代码中的参数
~~~

#### 具体的任务提交

以Standalone模式和Yarn模式为例，各写一个。

==Standalone模式虽然不用，但是它最能够代表Spark集群。==

~~~shell
#1.Standalone模式
$SPARK_HOME/bin/spark-submit \
--master spark://Master节点:7077,Master节点:7077 \
--deploy-mode client（Python中不支持cluster模式，Java和Scala支持） \
--driver-memory 8g \
--driver-cores 4 \
--executor-memory 8g \
--executor-cores 4 \
--total-num-executors 10 \
--jars mysql-connector-java-8.0.27.jar \
--conf spark.default.parallelism=3 \
--queue bxg \
--name bxg_bigdata \
xxxx.py



#2.Yarn模式
$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode client | cluster \
--driver-memory 8g \
--driver-cores 4 \
--executor-memory 8g \
--executor-cores 4 \
--num-executors 10 \
--jars mysql-connector-java-8.0.27.jar \
--conf spark.default.parallelism=3 \
--queue bxg \
--name bxg_bigdata \
xxxx.py



#3.Driver和Executor的配置说明
（1）Driver的数量：1个
（2）Executor的数量：理论上可以是任意，实际中可以设置为NodeManager的节点数。
（3）生产上的配置参考如下
--driver-memory 8g \
--driver-cores 4 \
--executor-memory 8g \
--executor-cores 4 \
--num-executors 50 \
~~~

### Spark的端口号

~~~shell
#1.4040
无论是什么运行模式，每一个Spark的任务，只要在运行期间，都有一个默认的端口号，这个端口号从4040开始。如果被占用，会依次往下递增。

#2.7077
Standalone模式下，Master接受任务的端口号，它只在独立模式下生效，不管是否有任务运行，只要独立模式的集群运行起来，这个端口号就开启了。

#8080
独立模式下，Spark集群的WebUI端口号，我们可以打开WebUI，查看独立模式的集群。

#18080
Spark的历史服务的端口号，如果需要开启Spark的历史服务，这个服务占用18080端口。它和运行模式没关系。查看日志的。
这个日志会配置HDFS的路径。
~~~

