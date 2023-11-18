# Flink项目

## 今日内容介绍

* metabase可视化（了解）
    * 如何快速了解一门陌生的技术

* 优化（不仅仅是SQL层面）

    * Flink的内存模型

    * Checkpoint

    * Flink的背压（反压）

    * 数据倾斜

* Flink复习（从基础到项目）

* Hive&Spark（下周）

## 可视化（了解）

### metabase介绍

目的：如何快速了解一门新技术。

可视化，在大数据中有很多种工具。包括开源和商业的。商业的就不提的。开源的工具中，github上点赞数最高的是Apache Superset。

其次，就是metabase。

metabase，是metabase公司的一个开源项目。这是一家商业公司。这个产品也叫metabase，它有开源的，也有商业的。

我们今天用的是这个公司的开源的metabase。

问题：如何快速了解这门新技术？

毫不疑问，官网是首选。

官网：www.metabase.com

点击：Get Started，如下图：

![1694843834224](assets/1694843834224.png)

点击：Jar,如下图：

![1694843931008](assets/1694843931008.png)

点击Jar File，如下图：

![1694844544454](assets/1694844544454.png)

这个页面就是metabase的使用说明：

~~~shell
#1.下载jar包
#2.创建一个新目录
#3.把jar包挪进去，然后运行命令：
java -jar metabase.jar
#4.访问浏览器
http://node1:3000/setup
~~~

当java命令运行后，稍等片刻，日志会出现：Metabase初始化完成字样，说明metabase启动成功。

启动成功后，访问web链接，如下图所示：

![1694844518892](assets/1694844518892.png)

到此，metabase启动成功。输入用户名和密码：

~~~shell
bigdata@itcast.cn
itcast1234
~~~

能成功登录，看到如下页面，说明metabase登录成功。

![1694844752107](assets/1694844752107.png)

### metabase使用

#### 配置

点击左下角的设置，选择管理员设置：

![1694844942892](assets/1694844942892.png)

点击创建数据库或者数据库，入下图：

![1694844993904](assets/1694844993904.png)

填写数据库的信息后，点击保存即可连接。如下图：

![1694845386033](assets/1694845386033.png)

配置完之后，就可以退出管理了。点击右上角`退出管理`按钮，如下图。

![1694846884658](assets/1694846884658.png)

**备注：配置数据库只需要配置一次即可。如果已经配好了，就不需要再配置了**

#### 使用

##### 创建SQL查询

SQL查询，就是看板中的指标。每新建一个SQL查询，就是在创建一个看板中的指标。设置完后，点击保存按钮，如下图所示：

![1694847131450](assets/1694847131450.png)

##### 创建仪表板

仪表板，就是看板。可以容纳多个SQL查询的指标。

看板中可以添加或者删除多个SQL查询。并且能设置刷新时间。

![1694847446219](assets/1694847446219.png)

一些其他更详细的使用方式，请参考文档。

## 优化

### 资源配置优化

#### 内存模型

Flink基础课中，说Flink实现了自己的内存管理。就是有自己的内存模型。

![1694848828704](assets/1694848828704.png)

内存模型，在实际中，一般推荐设置的是整个Flink内存大小，而不是在比较小的内存空间进行细分。

#### 并行度的设置

并行度的设置，Flink中有四种：

~~~shell
#1.配置文件中flink-conf.yaml，这种方式不推荐

#2.任务提交时，推荐使用
flink run-application yarn-application -p 3 xxx.jar

#3.全局环境层面，也不推荐
env.setParallelism(1)

#4.算子层面，也不推荐
sourceDS.flatMap().setParallelism(2)
~~~

并行度，就算不给，也有默认值，就是1。

#### Checkpoint的设置

Flink的Checkpoint的设置，时间不宜太短，也不宜太长。推荐就是分钟级别。

在设置Checkpoint的时候，有一些优化项。

~~~shell
#1.设置Checkpoint的本地回复，默认是false
state.backend.local-recovery

#2.设置任务取消时保存，默认是NO_EXTERNALIZED_CHECKPOINTS，建议设置为RETAIN_ON_CANCELLATION
execution.checkpointing.externalized-checkpoint-retention
#2.设置任务取消时保存数量，默认是1个，最好设置为2-3个
state.checkpoints.num-retained


#3.不要设置最大的Checkpoint
execution.checkpointing.tolerable-failed-checkpoints

#4.Checkpoint的并行的数量，默认就是1个，不要改这个值
execution.checkpointing.max-concurrent-checkpoints
~~~

#### 状态后端设置

Flink有三种状态后端：

* 内存
* 分布式文件系统
* RocksDB

内存不用。一般使用分布式文件系统。

如果是海量数据，吞吐量特别大，可以使用RocksDB的状态后端。

如果使用的是RocksDB的状态后端，请你额外注意：建议把磁盘设置为SSD，并且是单独挂载。

### Flink的背压（反压）

#### 什么是背压

背压，也叫反压，Backpressure。

~~~shell
（1）数据正常处理，算子能够处理持续不到到达的数据，这个时候没有反压
（2）当下游的算子处理不过来时，会向网络内存申请资源，存储数据，只要网络内存有资源，这会儿也不会有反压
（3）当数据仍然持续不到到达，网络内存资源被耗尽的时候，这个算子就会反向抑制上游的算子，降低上游算子数据写出频率，最终会导致整个任务无法正常处理数据。这就是反压。
~~~

如下图：

![1694852848508](assets/1694852848508.png)

#### 如何发现背压

直接去8081页面，观察任务的颜色和状态说明即可。

![1694853026226](assets/1694853026226.png)

![1694853204543](assets/1694853204543.png)

从8081页面上，查看算子的颜色图，可以看出反压。

算子的颜色有三种：

* 蓝色（很空闲）
* 红色（算子很忙碌）
* 黑色（有反压）

![1694853437421](assets/1694853437421.png)

- **OK**: 0% <= back pressured <= 10%
- **LOW**: 10% < back pressured <= 50%
- **HIGH**: 50% < back pressured <= 100%

#### 怎么处理背压

反压，在Flink中，是一种天然现象。基本上都会出现。**一般情况下都不用去处理**。

当反压频繁出现（经常出现）时，才需要处理。

当真正出现反压时，没有什么特别见效的方式，一般是和资源相关。

也可以通过参数来缓解一下。比如调整网络内存空间大小。还可以调整并行度的数量。

### 数据倾斜

#### 如何发现数据倾斜

在Flink的8081页面中，点开某一个任务，查看任务的SubTasks数据量，如果数据量都正常，则说明，没有出现数据倾斜，

如果有些SubTasks的数据量很大，有些SubTasks的数据量很小，则说明出现了数据倾斜。如下图：

没有出现倾斜的：

![1694855230345](assets/1694855230345.png)

出现数据倾斜的：

![1694855161715](assets/1694855161715.png)

#### 如何处理数据倾斜

如果数据本身出现倾斜，也就是说，在进行keyBy或者join之前。

可以通过数据倾斜的算子来处理：rebalance。

如果是因为keyBy或者join之后，出现了数据倾斜。那可以通过两阶段提交（map端预聚合）、分桶等方式来处理。

除此之外，给算子增加并行度，也可以减缓数据倾斜的情况。

### Kafka数据源

#### 动态分区检测

当Flink的数据源是Kafka时，我们一定要设置Kafka的分区数和Flink的并行度一致。

如果Kafka的分区数增大了，Flink最好要开启动态分区检测的功能，参数如下：

~~~shell
‘scan.topic-partition-discovery.interval’=’5000’
~~~

#### 设置空闲等待

有时候，Kafka有些并行度有数据，有些并行度没有数据，这种现象就是Watermark对齐。

如何解决Watermark对齐的问题，就是设置最大空闲等待时间。

~~~shell
#1.代码中
setIdleness(时间)

#2.SQL
table.exec.source.idle-timeout: 0 ms
~~~

### FlinkSQL的调优

FlinkSQL的调优，有一些，已经是调优好的，有些可以根据实际情况去设置。

#### 微批聚合

~~~shell
table.exec.mini-batch.enabled: true
table.exec.mini-batch.allow-latency: 5000 ms
table.exec.mini-batch.size: 1000
~~~

微批是看业务的。但是，一般情况都能允许短时间的延迟。

#### 两阶段聚合

~~~shell
table.optimizer.agg-phase-strategy: TWO_PHASE
~~~

#### 分桶

~~~shell
table.optimizer.distinct-agg.split.enabled: true
table.optimizer.distinct-agg.split.bucket-num: 1024
~~~

数据倾斜，优先采用两阶段聚合。如果两阶段效果不佳，则可以使用分桶。

#### filter子句

filter的优化体现在指标结果的优化上。

如果是传统的写法，则每一个指标都会保存一个状态。这样会把结果保存为3个状态。如果想获取最终结果，则需要读取3个不同的状态。效率低。

如果是filter的写法，它可以把上面的3个状态合并为一个共享的状态。只需要读取这一个状态就可以获取最终结果了。提升读取效率。





























