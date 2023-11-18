# Flink基础

## 今晚课程内容

## Flink的Checkpoint

* 为什么学Checkpoint
* Checkpoint概述
* Checkpoint的执行流程
* 案例演示
* 重启策略
* 状态后端
* 案例演示

### 为什么学Checkpoint

Flink是做流计算的，流式计算是没有结束的，任务是不会终止的。

也就是说，如果任务出现异常，Flink是如何保证数据的一致性的。

在流式计算中，Flink是通过Checkpoint机制来保证任务持续稳定运行的。

这就是我们学习Checkpoint机制的原因。

### Checkpoint概述

Checkpoint，又称为检查点机制，是为了Flink流式任务的容错的，也就是说，Flink流式任务在出现异常后，如何继续提供服务呢？

保证7*24小时不挂掉。

### Checkpoint的执行流程

![1692451824378](assets/1692451824378.png)

参考文字描述如下：

~~~shell
#1.检查点协调器（Checkpoint Coordinator）会定期发送一个一个的barrier（栅栏），这个barrier会混在数据中，随着数据流，一起流向source算子

#2.当Flink算子在处理数据时，正常处理就好

#3.当Flink算子在碰到Barrier（栅栏）时，它就会停下手里的工作，把当前的本地状态向主节点的检查点协调器汇报

#4.等状态汇报完后，这一个算子的Checkpoint就算做完了，Barrier会随着数据流，流向下一个算子

#5.下一个算子在接收到Barrier的时候，也会停下手里的工作，把本地的状态向主节点的检查点协调器进行汇报，以此类推...

#6.等Barrier把所有算子的状态都向主节点的检查点协调器汇报完后，这一轮的Checkpoint就算做完了

#7.等到下一个周期的时候，再继续下一个Checkpoint的状态汇报
~~~

> Checkpoint既然是用户自己设置的，一般设置多久合适呢？
>
> 答：一般建议分钟级别。不能太短。也不能太长。

### 思考：Flink有了Checkpoint就能实现容错吗？

不会。

如果任务怎么挂了怎么办。

也就是说，Checkpoint的全局状态，必须在任务没有挂掉的情况下才有用。

换言之，如果出现一些异常情况，必须保证Flink任务有一定的恢复策略才行。

Flink在遇到这种情况的时候，它内部提供了重启策略的机制，能够保证当出现问题的时候，进行自身恢复。

案例：

~~~java
package day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/19 21:43
 * @desc: Checkpoint演示
 */
public class Demo08_Checkpoint {
    public static void main(String[] args) throws Exception {
        //1.够建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置重启策略，这里指定为固定延迟重启策略，如果不设置重启策略，则任务会挂掉
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1));

        //2.数据源
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理
        //对数据进行flatMap扁平化处理
        SingleOutputStreamOperator<String> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    if (word.equals("shit")) {
                        throw new Exception("请不要说脏话...");
                    } else {
                        out.collect(word);
                    }
                }
            }
        });

        //把单词转换为(单词,1)的形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //对单词进行分组/分流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapDS.keyBy(value -> value.f0)
                .sum(1);

        //4.数据输出
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

### 重启策略

重启策略，英文是RestartStrategy。

Flink提供了4种重启策略：

* 不重启
* 固定延迟重启
* 失败率重启
* 指数延迟重启

#### 不重启

当Flink任务出现异常时，不重启。

配置文件中：

~~~shell
restart-strategy: none
~~~

Java代码写法：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.noRestart());
~~~

当没有设置重启策略时，Flink默认就是不重启策略。

#### 固定延迟重启

运行Flink任务固定重启多少次。默认是Integer.MAX_VALUE次。

一般不用默认值。因为通过一直重启来掩盖程序本身的问题，这不是一个明智的选择。同时CPU、内存等资源也会频繁创建和销毁。

配置文件中：

~~~shell
#配置固定延迟的重启策略
restart-strategy: fixed-delay
#允许重启的次数
restart-strategy.fixed-delay.attempts: 3
#每一次重启的时间间隔
restart-strategy.fixed-delay.delay: 10 s
~~~

Java代码写法：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, //允许重启3次
  Time.of(10, TimeUnit.SECONDS) //重启的间隔时间
));
~~~

#### 失败率重启

在一定的时间范围内，运行任务重启的次数。

配置文件中：

~~~shell
#配置失败率重启策略
restart-strategy: failure-rate
#在时间范围内，允许最大的失败次数
restart-strategy.failure-rate.max-failures-per-interval: 3
#时间范围，频率
restart-strategy.failure-rate.failure-rate-interval: 5 min
#每一次重启的时间间隔
restart-strategy.failure-rate.delay: 10 s
~~~

Java代码中：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, //在时间范围内，允许最大的失败次数
  Time.of(5, TimeUnit.MINUTES), //时间范围，频率
  Time.of(10, TimeUnit.SECONDS) //#每一次重启的时间间隔
));
~~~

#### 指数延迟重启

指数延迟重启策略，重启时间会随着指数的递增而递增。

配置文件中：

~~~shell
#配置指数重启策略
restart-strategy: exponential-delay
#重启间隔时间的初始值
restart-strategy.exponential-delay.initial-backoff: 10 s
#最大的重启间隔时间
restart-strategy.exponential-delay.max-backoff: 2 min
#指数，表示以该指数递增
restart-strategy.exponential-delay.backoff-multiplier: 2.0
#重置的时间，当任务稳定运行最大阈值时间后，如果没有出问题，则重启时间会重置为初始值
restart-strategy.exponential-delay.reset-backoff-threshold: 10 min
#抖动因子，为了防止大量的任务在同一时刻重启
restart-strategy.exponential-delay.jitter-factor: 0.1
~~~

Java代码的写法：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
  Time.milliseconds(1),//重启间隔时间的初始值
  Time.milliseconds(1000),//最大的重启间隔时间
  1.1, //指数，表示以该指数递增
  Time.milliseconds(2000), //阈值
  0.1 //抖动因子
));
~~~

#### 小结

公司中，一般使用固定延迟或者失败率重启策略。

### 状态后端

状态后端，英文StateBackends，可以保存Flink在进行流式计算过程中的中间状态。

Flink的状态后端有三种：

* MemoryStateBackend（内存状态后端）
* FsStateBackend（文件系统状态后端）
* RocksDBStateBackend（RocksDB状态后端）

#### 内存状态后端

内存状态后端，是默认的状态后端。保存在JobManager的内存中。

![1692706586144](assets/1692706586144.png)

配置文件中：

~~~shell
state.backend: hashmap
state.checkpoint-storage: jobmanager
~~~

Java代码的写法：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
~~~

开发测试中使用，生产上不用。

#### 文件系统状态后端

可以把状态存储在文件系统上。而不是在内存中。

文件系统：本地文件系统，也可以是分布式文件系统。推荐使用分布式文件系统。

![1692706759832](assets/1692706759832.png)

配置文件中：

~~~shell
state.backend: hashmap
state.checkpoints.dir: file:///checkpoint-dir/
state.checkpoint-storage: filesystem
~~~

Java代码的写法：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));
~~~

生产上常用。

#### RocksDB状态后端

RocksDB：DB：数据库。RocksDB是Rocks数据库。

![1692707037553](assets/1692707037553.png)

这个数据库需要安装吗？

不需要。

但是需要引入RocksDB的依赖：

~~~properties
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb</artifactId>
    <version>1.15.4</version>
    <scope>provided</scope>
</dependency>
~~~

特点：

支持超大状态、增量存储。效率很高。一般是海量数据场景下才使用。

配置文件中：

~~~shell
state.backend: rocksdb
state.checkpoints.dir: file:///checkpoint-dir/
state.checkpoint-storage: filesystem
~~~

Java代码的写法：

~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new EmbeddedRocksDBStateBackend());
env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));
~~~

### 比较完整的Checkpoint配置

Java代码中：

~~~java
//Chcekpoint配置
//Checkpoint开启
env.enableCheckpointing(1000);
//设置Checkpoint的执行模式为精准一次，默认就是精准一次
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//设置两次Checkpoint之间的最小间隔
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//设置Checkpoint超时时间
env.getCheckpointConfig().setCheckpointTimeout(60000);
//设置允许Checkpoint失败的次数
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
//设置同时允许Checkpoint的并发数
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//设置任务取消后，Checkpoint的数据是删除还是保留，建议配置为保留
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//设置Checkpoint的存储路径
env.getCheckpointConfig().setCheckpointStorage("file:///D:\\checkpoint");
~~~

配置文件中：

~~~shell
execution.checkpointing.interval: 5000
#设置有且仅有一次模式 目前支持 EXACTLY_ONCE、AT_LEAST_ONCE        
execution.checkpointing.mode: EXACTLY_ONCE
state.backend: hashmap
#设置checkpoint的存储方式
state.checkpoint-storage: filesystem
#设置checkpoint的存储位置
state.checkpoints.dir: hdfs://node1:8020/checkpoints
#设置savepoint的存储位置
state.savepoints.dir: hdfs://node1:8020/checkpoints
#设置checkpoint的超时时间 即一次checkpoint必须在该时间内完成 不然就丢弃
execution.checkpointing.timeout: 600000
#设置两次checkpoint之间的最小时间间隔
execution.checkpointing.min-pause: 500
#设置并发checkpoint的数目
execution.checkpointing.max-concurrent-checkpoints: 1
#开启checkpoints的外部持久化这里设置了清除job时保留checkpoint，默认值时保留一个 假如要保留3个
state.checkpoints.num-retained: 3
#默认情况下，checkpoint不是持久化的，只用于从故障中恢复作业。当程序被取消时，它们会被删除。但是你可以配置checkpoint被周期性持久化到外部，类似于savepoints。这些外部的checkpoints将它们的元数据输出到外#部持久化存储并且当作业失败时不会自动
清除。这样，如果你的工作失败了，你就会有一个checkpoint来恢复。
#ExternalizedCheckpointCleanup模式配置当你取消作业时外部checkpoint会产生什么行为:
#RETAIN_ON_CANCELLATION: 当作业被取消时，保留外部的checkpoint。注意，在此情况下，您必须手动清理checkpoint状态。
#DELETE_ON_CANCELLATION: 当作业被取消时，删除外部化的checkpoint。只有当作业失败时，检查点状态才可用。
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
~~~

### 综合案例

~~~java
package day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author: itcast
 * @date: 2023/8/19 21:43
 * @desc: Checkpoint演示
 */
public class Demo08_Checkpoint {
    public static void main(String[] args) throws Exception {
        //1.够建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Chcekpoint配置
        //Checkpoint开启
        env.enableCheckpointing(1000);
        //设置Checkpoint的执行模式为精准一次，默认就是精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置两次Checkpoint之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置Checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置允许Checkpoint失败的次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //设置同时允许Checkpoint的并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置任务取消后，Checkpoint的数据是删除还是保留，建议配置为保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置Checkpoint的存储路径
        env.getCheckpointConfig().setCheckpointStorage("file:///D:\\checkpoint");

        //设置重启策略为3次，每次间隔1秒钟
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, //允许重启3次
                Time.of(1, TimeUnit.SECONDS) //重启的间隔时间
        ));

        //2.数据源
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理
        //对数据进行flatMap扁平化处理
        SingleOutputStreamOperator<String> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    if (word.equals("shit")) {
                        throw new Exception("请不要说脏话...");
                    } else {
                        out.collect(word);
                    }
                }
            }
        });

        //把单词转换为(单词,1)的形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //对单词进行分组/分流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapDS.keyBy(value -> value.f0)
                .sum(1);

        //4.数据输出
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

## SQL

FlinkSQL是Apache Calcite框架解析的，也是标准SQL。大部分都相同。

### 建库建表

#### 语法

建库和MySQL一样，建表比较特殊，我们重点看看建表。

~~~shell
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
  [ LIKE source_table [( <like_options> )] ]
   
<physical_column_definition>:
  column_name column_type [ <column_constraint> ] [COMMENT column_comment]
  
<column_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY NOT ENFORCED

<table_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<metadata_column_definition>:
  column_name column_type METADATA [ FROM metadata_key ] [ VIRTUAL ]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

<source_table>:
  [catalog_name.][db_name.]table_name

<like_options>:
{
   { INCLUDING | EXCLUDING } { ALL | CONSTRAINTS | PARTITIONS }
 | { INCLUDING | EXCLUDING | OVERWRITING } { GENERATED | OPTIONS | WATERMARKS } 
}[, ...]
~~~

解读：

~~~shell
[可选内容]：可以写，也可以不写
(必选内容)：小括号不能省，里面不要为空
{多选内容}：可以从中选择一部分
<标记内容>：这里只是做个标记，下面会有详细的解释说明
|：或者的含义
~~~

#### 连接器Connector

连接器：决定数据的位置。

~~~shell
#1.socket连接器，可以读取socket数据
('connector' = 'socket',
'hostname' = 'node1',
'port' = '9999',
'format' = 'csv')


#2.print连接器，可以把数据输出到标准输出
('connector' = 'print')


#3.upsert-kafka连接器，连接Kafka数据源
('connector' = 'upsert-kafka',
'topic' = 'test',
'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092',
'key.format' = 'avro',
'value.format' = 'avro')


#4.datagen连接器，可以模拟数据源，源源不断地产生数据
('connector' = 'datagen',
'number-of-rows' = '10',
'fields.#.kind' = 'random')


#5.filesystem连接器，连接文件
('connector'= 'filesystem',
'path' = '/root/test.txt',
'format' = 'csv')


#6.jdbc连接器，连接jdbc数据源
('connector' = 'jdbc',
'url' = 'jdbc:mysql://localhost:3306/mydatabase',
'table-name' = 'users',
'username' = 'root',
'password' = '123456')
~~~

#### format数据格式

压缩的文件格式：orc、parquet

非压缩的文件格式：csv、json

#### watermark的定义

~~~shell
WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

#解释
watermark for 事件时间列 as 事件时间列 - 延迟时间
watermark for rt as rt - interval '5' second
~~~

#### 主键

~~~shell
PRIMARY KEY NOT ENFORCED

#解释
Flink的主键和MySQL的主键不同。MySQL可以存储数据，Flink不存储数据。因此它不会校验数据的唯一性。所以必须加上not enforced
primary key not enforced
~~~

### CTE语句

Flink支持with as的语法糖。

~~~sql
-- 语法糖+1
WITH orders_with_total AS (
    SELECT 
        order_id
        , price + tax AS total
    FROM Orders
)
SELECT 
    order_id
    , SUM(total)
FROM orders_with_total
GROUP BY 
    order_id;
~~~

### select & where语句

和MySQL一样，具体使用略。

### distinct语句

使用和MySQL一样，略。

### 窗口

滚动、滑动、会话等窗口在前面单独讲过，这里不重复了。

### group语法糖

#### grouping sets

~~~sql
SELECT supplier_id, rating, COUNT(*) AS total
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())



--上述SQL中的groupp by等价于


SELECT supplier_id, rating, COUNT(*) AS total
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY (supplier_id, rating)
union
SELECT supplier_id, rating, COUNT(*) AS total
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY (supplier_id)
union
SELECT supplier_id, rating, COUNT(*) AS total
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY ()
~~~

#### rollup

```sql
SELECT supplier_id, rating, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY ROLLUP (supplier_id, rating)



--上述SQL等价于



SELECT supplier_id, rating, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY supplier_id
union
SELECT supplier_id, rating, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY (supplier_id, rating)
union 
SELECT supplier_id, rating, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY ()

```

#### cube

```sql
SELECT supplier_id, rating, product_id, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY CUBE (supplier_id, rating, product_id)



--上述的SQL等价于下面的SQL



SELECT supplier_id, rating, product_id, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SET (
    ( supplier_id, product_id, rating ),
    ( supplier_id, product_id         ),
    ( supplier_id,             rating ),
    ( supplier_id                     ),
    (              product_id, rating ),
    (              product_id         ),
    (                          rating ),
    (                                 )
)
```

### Join

Flink的Join分为好几种：

* Regular Join
* Interval Join
* Lookup Join

#### Regular Join

Regular Join，也叫常规Join。也就是说，和Hive、MySQL的Join是一样的。

Regular Join也可以分为三种情况：

* inner join

~~~sql
--日志表
CREATE TABLE show_log_table (
    log_id BIGINT,
    show_params STRING
) WITH (
  'connector' = 'socket',
  'hostname' = 'node1',        
  'port' = '8888',
  'format' = 'csv'
);


--点击数据表
CREATE TABLE click_log_table (
  log_id BIGINT,
  click_params     STRING
) WITH (
  'connector' = 'socket',
  'hostname' = 'node1',        
  'port' = '9999',
  'format' = 'csv'
);


--数据处理
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table
INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;
~~~

截图如下：

![1692712266218](assets/1692712266218.png)

* left join/right join

~~~sql
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table
left JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;
~~~

截图如下：

![1692712501350](assets/1692712501350.png)

* full join

~~~sql
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table
full JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;
~~~

截图如下：

![1692712623469](assets/1692712623469.png)

总结：Regular Join和普通的Hive Join类似。

#### Interval Join

Interval Join，就是在Regular Join的基础之上，加上了时间区间的条件。

同样，它也可以分为三种情况：

* Inner Join

~~~shell
#1.日志表
CREATE TABLE show_log_table (
    log_id BIGINT,
    show_params STRING,
    `timestamp` bigint,
    row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
    watermark for row_time as row_time 
) WITH (
  'connector' = 'socket',
  'hostname' = 'node1',        
  'port' = '8888',
  'format' = 'csv'
);


#2.点击数据表
CREATE TABLE click_log_table (
  log_id BIGINT,
  click_params     STRING,
  `timestamp` bigint,
  row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
  watermark for row_time as row_time 
)
WITH (
  'connector' = 'socket',
  'hostname' = 'node1',        
  'port' = '9999',
  'format' = 'csv'
);



#3.数据处理
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table 
INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id
AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '10' SECOND AND click_log_table.row_time;
~~~

分析：

~~~shell
#假设左表的第一条数据是
1001,hive,1

#假设右表的第一条数据是
1001,zhangsan,1
右表的时间区间为：
[1-10,1]  -> [-9,1] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第二条数据是
1001,lisi,5
右表的时间区间为：
[5-10,5] -> [-5,5] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第三条数据是
1001,wangwu,10
右表的时间区间为：
[10-10,10] -> [0,10] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第四条数据是
1001,zhaoliu,11
右表的时间区间为：
[11-10,11] -> [1,11] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第五条数据是
1001,tianqi,12
右表的时间区间为：
[12-10,12] -> [2,12] -> 不能包含左表的事件时间范围，因此这条数据无法Join上
~~~

截图如下：

![1692878489802](assets/1692878489802.png)

* Left/Right Join

~~~shell
#1.数据处理
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table 
left JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id
AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '5' SECOND AND click_log_table.row_time + Interval '5' second;
~~~

分析：

~~~shell
#假设左表的第一条数据是
1001,hive,1

#假设右表的第一条数据是
1001,zhangsan,5
右表的时间区间为：
[5-5,5+5]  -> [0,10] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第二条数据是
1001,lisi,6
右表的时间区间为：
[6-5,6+5] -> [1,11] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第三条数据是
1001,wangwu,7
右表的时间区间为：
[7-5,7+5] -> [2,12] -> 不能包含左表的事件时间范围，因此这条数据无法Join上
~~~

截图如下：

![1692879813923](assets/1692879813923.png)

* Full Join

~~~shell
#1.数据处理
SELECT
    show_log_table.log_id as s_id,
    show_log_table.show_params as s_params,
    click_log_table.log_id as c_id,
    click_log_table.click_params as c_params
FROM show_log_table 
full JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id
AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '5' SECOND AND click_log_table.row_time + Interval '5' second;
~~~

分析：

~~~shell
#假设左表的第一条数据是
1001,hive,1

#假设右表的第一条数据是
1001,zhangsan,5
右表的时间区间为：
[5-5,5+5]  -> [0,10] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第二条数据是
1001,lisi,6
右表的时间区间为：
[6-5,6+5] -> [1,11] -> 包含左表的事件时间范围，因此这条数据应该能Join上
#假设右表的第三条数据是
1001,wangwu,7
右表的时间区间为：
[7-5,7+5] -> [2,12] -> 不能包含左表的事件时间范围，因此这条数据无法Join上
~~~

截图如下：

![1692880624468](assets/1692880624468.png)

#### Lookup Join

Lookup Join，也称为维表Join，也就是说，它可以读取（Join）其他存储介质中的数据，当做维度表使用。

比如：MySQL中的数据。

案例：通过Lookup Join，来关联MySQL中维度表数据。

~~~shell
#1.建库
create database test;


#2.切换库
use test;


#3.建表
CREATE TABLE `user_profile` (
  `user_id` varchar(100) NOT NULL,
  `age` varchar(100) DEFAULT NULL,
  `sex` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


#4.插入数据
INSERT INTO test.user_profile (user_id,age,sex) VALUES
	 ('a','12-18','男'),
	 ('b','18-24','女'),
	 ('c','18-24','男');


#5.创建FlinkSQL中的表（点击日志表）
CREATE TABLE click_log_table (
  log_id BIGINT, 
  `timestamp` bigint,
  user_id string,
  proctime AS PROCTIME()
)
WITH (
  'connector' = 'socket',
  'hostname' = 'node1',        
  'port' = '8888',
  'format' = 'csv'
);

#6.在FlinkSQL创建MySQL中的user_profile表的映射表
CREATE TABLE user_profile (
  `user_id` string, 
  `age` string,
  `sex` string
)
WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://node1:3306/test',
   'table-name' = 'user_profile',
   'username'='root',
   'password'='123456'
);


#7.数据处理SQL
SELECT 
    s.log_id as log_id, 
    s.`timestamp` as `timestamp`, 
    s.user_id as user_id, 
    s.proctime as proctime, 
    u.sex as sex, 
    u.age as age
FROM click_log_table AS s
LEFT JOIN user_profile u ON s.user_id = u.user_id;
~~~

截图如下：

![1692882952270](assets/1692882952270.png)

### 集合操作

Flink的集合操作有并集（union）、交集（intersect）、差集（except）。

~~~shell
#1.创建视图
create view t1(s) as values ('c'), ('a'), ('b'), ('b'), ('c');
create view t2(s) as values ('d'), ('e'), ('a'), ('b'), ('b');
~~~

#### union并集

~~~shell
#演示union
(SELECT s FROM t1) UNION (SELECT s FROM t2);

#union结果
c
a
b
d
e


#演示union all
(SELECT s FROM t1) UNION ALL(SELECT s FROM t2);

#union all结果
d
e
a
b
b
c
a
b
b
c
~~~

#### intersect交集

~~~shell
#演示intersect
(SELECT s FROM t1) intersect (SELECT s FROM t2);


#intersect的结果
a
b

#演示intersect all
(SELECT s FROM t1) intersect all(SELECT s FROM t2);


#intersect all的结果
a
b
b
~~~

#### except差集

~~~shell
#演示except
(SELECT s FROM t1) except (SELECT s FROM t2);


#except的结果
c

#演示except all
(SELECT s FROM t1) except all(SELECT s FROM t2);


#except all的结果
c
c
~~~

截图如下：

![1692883546533](assets/1692883546533.png)

### order by & limit子句

和MySQL、Hive类似。

### TopN& Deduplicate

TopN：使用row_number来进行求TopN的操作。和Hive类似。

~~~shell
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = N
~~~

Deduplicate，就是去重操作，可以使用row_number=1来进行去重。也和Hive类似。

~~~shell
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = 1


select id, name 
  from (select id, name, 
               row_number() over(partition by id order by id) rowid 
          from employee) a
where rowid = 1
~~~

### explain查询计划

Flink的查询计划相比Spark而言，会显得比较人性化，也容易读懂。

~~~shell
CREATE TABLE source_table (
    user_id BIGINT COMMENT '用户 id',
    name STRING COMMENT '用户姓名',
    server_timestamp BIGINT COMMENT '用户访问时间戳',
    proctime AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.name.length' = '1',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '10',
  'fields.server_timestamp.min' = '1',
  'fields.server_timestamp.max' = '100000'
);


EXPLAIN 
select user_id,
       name,
       server_timestamp
from (
      SELECT
          user_id,
          name,
          server_timestamp,
          row_number() over(partition by user_id order by proctime) as rn
      FROM source_table
)
where rn = 1;


或者这么写也可以



EXPLAIN plan for 
select user_id,
       name,
       server_timestamp
from (
      SELECT
          user_id,
          name,
          server_timestamp,
          row_number() over(partition by user_id order by proctime) as rn
      FROM source_table
)
where rn = 1;
~~~

截图如下：

![1692884526176](assets/1692884526176.png)

### use & show子句

元数据库：FlinkSQL把元数据库的控制权交给用户了，和永久表结合使用，项目中演示。

FlinkSQL的层级关系为：元数据库-> 数据库-> 表

数据库：和MySQL、Hive的数据库类似，用来管理表。

~~~shell
#1.查看元数据库
show catalogs;

#2.切换元数据库
use catalog default_catalog;

#3.查看数据库
show databases;

#4.切换数据库
use default_database;

#5.查看表
show tables;

#6.查看函数
show functions;

#7.查看视图
show views;

#8.查看当前所在的元数据库
show current catalog;

#9.#8.查看当前所在的数据库
show current database;
~~~

截图如下：

![1692885226910](assets/1692885226910.png)













