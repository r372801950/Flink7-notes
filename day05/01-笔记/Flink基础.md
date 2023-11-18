# Flink基础

## 今日课程内容介绍

* 流式表环境对象介绍（StreamTableEnvironment）
* 数据类型
* 动态表&连续查询
* 时间语义【掌握】
* 窗口

## 流式表环境对象介绍

### 功能介绍

~~~shell
#1.元数据库
MySQL中，表的路径（层次关系）为：数据库名.表名，默认的数据库为default
FlinkSQL中，表的路径（层次关系）为：元数据库.数据库名.表名
这个元数据库是由用户自定义的。和Hive完全不同。
Hive的元数据库：MySQL
Flink的元数据库：在Flink中，由用户自定义，用户自己来管理。
为什么这么设计呢？
Flink是专注于计算。而不关心数据在那里，也不关心数据计算完后存储到哪里去。
Flink中的元数据库默认为catalog_default
包括元数据库的创建、删除。

#2.数据库
Flink也有数据库，和MySQL、Hive一样。默认的数据库名称为：default_database
包括数据库的创建、删除等。

#3.管理表
Flink也可以创建表。和其他的数据库类似。没有默认的表。包括表的创建、删除等。
表的层级：元数据库.数据库.表，所以默认为：default_catalog.default_database.table

#4.视图
FlinkSQL也可以管理视图，包括视图的创建、删除等

#5.函数
FlinkSQL也和其他数据库类似，提供了大量丰富的内置函数。以供使用者自由选择。
~~~

### 常用的API介绍

~~~shell
#1.executeSql
执行SQL的API，参数中带上SQL
这个API可以执行任意的SQL语句。

#2.createTemporaryTable、createTable
创建临时表

#3.from
读取某个表

#4.getConfig
获取配置对象
~~~

### FlinkSQL中的表

Flink中的表有几类？

~~~shell
#1.临时表
临时表，顾名思义，这个表是临时的。只在当前会话中有效。只要会话退出，表就消失。
它只存在于当前会话中。


#2.永久表
可以永久存储，不会随着会话的退出而消失。可以在多个会话中访问使用。
永久表需要结合catalog来创建（元数据库来使用）


#3.外部表
外部表，主要是用来链接外部数据源的。
Flink是一个计算框架，它不保存数据，通过连接器（connector）来链接外部的数据源。



#4.小结
临时表：用的最多，一般结合外部表一起使用。
永久表：用的较少。它需要结合catalog来使用。
外部表：用的较多。结合临时表一起使用。
~~~

### 分层API

![1691907115692](assets/1691907115692.png)

![1691907139245](assets/1691907139245.png)

汇总如下：

~~~shell
#1.最顶层
SQL、TableAPI

#2.核心层
DataStream API，DataSet API（不用了）

#3.最底层
状态处理，时间，窗口等
~~~

### FlinkSQL客户端

#### 客户端简介

~~~shell
#1.启动FlinkSQL客户端
cd $FLINK_HOME
bin/sql-client.sh

#2.执行测试语句,这里要注意，下面三个示例，任何一个运行成功都表示FlinkSQL正常
select 'Hello World';
select "Hello World";
select 1;
~~~

截图如下：

![1691909112214](assets/1691909112214.png)

#### 显示模式

FlinkSQL有三种显示模式：

~~~shell
#1.Table模式
默认的模式，它会打开一个新的黑窗口
set sql-client.execution.result-mode = table;
select 'Hello World';

#2.changelog模式
变更日志的模式，会有数据的变更标识，比如+I等，也会打开一个新的黑窗口
set sql-client.execution.result-mode = changelog;
select 'Hello World';

#3.tableau模式
传统数据库的模式，不会打开新的黑窗口，只在当前窗口中打开
set sql-client.execution.result-mode = tableau;
select 'Hello World';
~~~

table模式截图：

![1691909758679](assets/1691909758679.png)

changelog模式截图：

![1691909665537](assets/1691909665537.png)

tableau模式：

![1691909731241](assets/1691909731241.png)

这三种显示模式，根据自己的个人喜好选择即可。

### FlinkSQL入门案例

纯SQL，没有一行代码。

~~~shell
  #1.构建source表
create table source_table (
 word string
 ) with (
 'connector' = 'socket',
 'hostname' = 'node1',
 'port' = '9999',
 'format' = 'csv'
 );


#2.构建sink表
create table sink_table (
 word string,
 counts bigint
 ) with (
 'connector' = 'print'
 );
 
 
#3.拉起数据任务
insert into sink_table select word,count(1) from source_table group by word;


#4.开启socket
nc -lk 9999
~~~

截图如下：

![1691910780981](assets/1691910780981.png)

### FlinkSQL综合案例

这个综合案例，我们重点介绍connector的使用。

~~~shell
#1.需求
计算每一种商品（sku_id 唯一标识）的售出个数、总销售额、平均销售额、最低价、最高价
使用数据源模拟器来生成数据，并且计算后写入到Kafka中。

#datagen
可以模拟数据源，来源源不断地产生数据。
create table source (
sku_id string,
price int
) with (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.sku_id.kind' = 'random',
'fields.sku_id.length' = '4',
'fields.price.kind' = 'random',
'fields.price.min' = '1',
'fields.price.max' = '100'
);

#kafka
#FlinkSQL的主键，不会去校验数据的一致性，它是不强制性的。
create table sink (
sku_id string,
sales_counts bigint,
sales_sum int,
sales_avg double,
sales_min int,
sales_max int,
primary key (sku_id) not enforced
) with (
'connector' = 'upsert-kafka',
'topic' = 'test',
'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092',
'key.format' = 'json',
'value.format' = 'json'
);


#启动Kafka（3个节点都运行如下命令）
zkServer.sh  start
cd $KAFKA_HOME
nohup bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log &


#查看Kafka中的topic
bin/kafka-topics.sh --list --bootstrap-server node1:9092,node2:9092,node3:9092

#删除原有的test topic
bin/kafka-topics.sh --delete --topic test  --bootstrap-server node1:9092,node2:9092,node3:9092

#创建test topic
bin/kafka-topics.sh --create --topic test  --bootstrap-server node1:9092,node2:9092,node3:9092

#数据任务
insert into sink
select 
sku_id,
count(sku_id),
sum(price),
avg(price),
min(price),
max(price) 
from source group by sku_id;


#复制Kafka的jar包到FLINK_HOME/lib目录下
cp /export/software/flink-sql-connector-kafka-1.15.2.jar $FLINK_HOME/lib
重启Flink集群，再进入到FlinkSQL客户端中，执行SQL即可。
~~~

任务提交截图：

![1691914568450](assets/1691914568450.png)

Kafka Tool截图：

![1691914555434](assets/1691914555434.png)

## 数据类型

FlinkSQL中类型可以分为两类：

* 基础数据类型（原子数据类型）
* 复合数据类型

### 基础数据类型

~~~shell
#1.字符
char

#2.字符串类型
varchar
string

#3.数值类型
int
bigint
smallint
decimal

#4.浮点型
float
double

#5.null类型
null

#6.boolean类型
true
false


#7.时间&日期类型
date
time
datetime
timestamp：不带时间戳
timestamp_ltz：带时间戳，ltz：local time zone的缩写
~~~

### 复合数据类型

~~~shell
#1.数组类型
array

#2.map类型
map

#3.集合类型
multiset

#4.对象类型
row
~~~

### 案例

~~~shell
#1.创建表
CREATE TABLE json_source (
    id            BIGINT,
    name          STRING,
    `date`        DATE,
    obj           ROW<time1 TIME,str STRING,lg BIGINT>,
    arr           ARRAY<ROW<f1 STRING,f2 INT>>,
    `time`        TIME,
    `timestamp`   TIMESTAMP(3),
    `map`         MAP<STRING,BIGINT>,
    mapinmap      MAP<STRING,MAP<STRING,INT>>,
    proctime as PROCTIME()
 ) WITH (
    'connector' = 'socket',
    'hostname' = 'node1',        
    'port' = '9999',
    'format' = 'json'
);


#2.查询SQL
select id, name,`date`,obj.str,arr[1].f1,`map`['flink'],mapinmap['inner_map']['key'] from json_source;


#3.数据
{"id":1238123899121,"name":"itcast","date":"1990-10-14","obj":{"time1":"12:12:43","str":"sfasfafs","lg":2324342345},"arr":[{"f1":"f1str11","f2":134},{"f1":"f1str22","f2":555}],"time":"12:12:43","timestamp":"1990-10-14 12:12:43","map":{"flink":123},"mapinmap":{"inner_map":{"key":234}}}
~~~

截图如下：

![1691916544724](assets/1691916544724.png)

## 动态表&连续查询

![1691916756896](assets/1691916756896.png)

FlinkSQL中的表，都是动态表。也就是说，FlinkSQL中的表，会随着数据的动态变化而变化。

连续查询：指的是查询不是一次性的。是可以持续不断地。因此，称为连续查询。

Flink：流

FlinkSQL：表（？）

![1691917059120](assets/1691917059120.png)

总结：

Flink的流和表是对等的。流可以转换为表，表也可以认为是流在某个时刻的瞬时结果。

## 时间语义

![1691918360394](assets/1691918360394.png)

Flink中的时间有三类：

* 摄入时间
* 处理时间
* 事件时间

### 摄入时间

数据被Flink程序摄入（读取）的时间。这个时间是由Flink决定的。

这个时间不用。

### 处理时间

数据被Flink程序处理的时间，一般会以处理完成为准。这个时间是由Flink决定的。

这个时间很少使用。

### 事件时间

事件：一系列操作（Web终端操作，日志，功能）

在事件中数据真正产生的时间。这个时间和Flink没关系。**这个时间是数据产生时携带的时间**。

这个时间用的最多。

### 定义

摄入时间，在FlinkSQL没有定义。

它只有处理时间和事件时间的定义。

#### 处理时间

~~~shell
#1.定义
采用proctime()函数定义
pt as proctime()

#2.案例
create table InputTable (
`userid` varchar,
`timestamp` bigint,
`money` double,
`category` varchar,
`pt` AS PROCTIME()
) with (
'connector' = 'filesystem',
'path' = 'file:///export/data/input/order.csv',
'format' = 'csv'
);

#3.解释
上述表中的pt列就是处理时间的定义。


#4.文件全路径为/export/data/input/order.csv
1001,1645174744,4999,电脑
1002,1645174745,1699,沙发
1003,1645174747,299,桌子
1004,1645174750,150,水杯
1005,1645174753,2800,相机
1006,1645174760,2000,平板
1007,1645175760,3000,手提
~~~

截图入戏：

![1691918637068](assets/1691918637068.png)

#### 事件时间

~~~shell
#1.定义
需要结合watermark来定义。
WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
watermark for 事件时间列 as 策略表达式(延迟时间)
watermark for 事件时间列 as 事件时间列 - 延迟时间
具体关于watermark的使用，后面在水印中会详细介绍。
现在只需要使用即可。
如：
watermark for rtime as rtime - interval '0' second
interval '0' second：延迟时间，0表示不延迟。效果和普通表一样。
rtime：事件时间列，名字随意，但是必须要是timestamp或者timestamp_ltz类型。


#2.定义
create table InputTable2 (
`userid` varchar,
`timestamp` bigint,
`money` double,
`category` varchar,
rt AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
watermark for rt as rt - interval '0' second
) with (
'connector' = 'filesystem',
'path' = 'file:///export/data/input/order.csv',
'format' = 'csv'
);


#3.说明
watermark for rt as rt - interval '0' second
这个写法和没有延迟是一样的。就把它当做普通表用即可。不用关注延迟的事。


#4.注意
事件时间列必须是timestamp或者timestamp_ltz类型。
如果不是，则可以使用to_timestamp函数转换
~~~

截图如下：

![1691919032890](assets/1691919032890.png)

==总结：时间是为窗口服务的。==

## Flink的窗口

Flink中的窗口有很多，我们从如下的顺序来学习：

* 窗口

* **滚动窗口**
* **滑动窗口**
* **会话窗口**
* 渐进式窗口
* 聚合窗口

### 窗口

#### 为什么学窗口

![1691927502736](assets/1691927502736.png)

流计算的类型可以分为两种：

* 从开始就有的，一直没有结束
* 一段时间之内的

第一种需求虽然有，但是不多。Flink也不是重点在这一块。，生活中的这种案例也不常见。

流式处理几乎都是第二种类型的需求。比如：统计一段时间内的指标。

一段时间内，在Flink中就称之为一个：**窗口**。

这就是一段时间内的数据，也就是一个批数据。

所以，**窗口本质就是把流式计算转换为批量计算的桥梁。**

这也是我们学窗口的原因。

#### 生活中的窗口

![1691929079613](assets/1691929079613.png)

* 大小固定（长宽大小）

* 有边界（上下左右四个边界）
* 连通的（连接室内和室外的桥梁）

#### 程序中的窗口

![1691929604754](assets/1691929604754.png)

* 有大小（时间区间范围）
* 有边界（左右边界）
* 是流式计算转换为批量计算的桥梁

#### Flink中的窗口

窗口的概念，不是Flink特有的。Spark的StructuredStreaming也有窗口。这里特指Flink的窗口。

Flink的窗口类型如下：

* **滚动窗口**
* **滑动窗口**
* **会话窗口**
* 渐进式窗口
* 聚合窗口

### 滚动窗口

#### 概述

![1691930219295](assets/1691930219295.png)

概念：窗口大小 = 滚动/滑动距离。

特点：数据不重复，不丢失。

前端修改配置项，重新出arm前端镜像

分辨率适配

#### SQL入门案例

~~~shell
#1.创建表
CREATE TABLE source_table ( 
 user_id STRING, 
 price BIGINT,
 `timestamp` bigint,
 row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
 watermark for row_time as row_time - interval '0' second
) WITH (
  'connector' = 'socket',
  'hostname' = 'node1',        
  'port' = '9999',
  'format' = 'csv'
);


#2.窗口的SQL定义
(1)滚动窗口，tumble(事件时间列, 窗口大小)
窗口大小根据业务来定。比如：10分钟，1小时都可以。
(2)把窗口放在分组（group by）后面即可
比如：group by tumble(row_time, interval '5' second)


#3.数据处理SQL
select 
user_id,
count(*) as pv,
sum(price) as sum_price,
UNIX_TIMESTAMP(CAST(tumble_start(row_time, interval '5' second) AS STRING)) * 1000  as window_start,
UNIX_TIMESTAMP(CAST(tumble_end(row_time, interval '5' second) AS STRING)) * 1000  as window_end
from source_table
group by
    user_id,
    tumble(row_time, interval '5' second);
~~~

截图如下：

![1691932633209](assets/1691932633209.png)

#### 窗口的起始

窗口的起始，就是窗口的起始时间。这个是怎么计算的呢？

这里是滚动窗口，因此当第一个窗口的起始时间和结束时间确定好了后，整个窗口的排布也就确定了。

计算公式如下：

~~~shell
窗口的起始时间 = 第一条数据的事件时间 - （第一条数据的事件时间 % 窗口大小）
~~~

演算如下：

~~~shell
#1.第一个窗口
起始时间 = 第一条数据的事件时间 - （第一条数据的事件时间 % 窗口大小）
       = 1 - （1 % 5）
       = 1 - 1
       = 0
起始时间是从0秒开始。
~~~

#### 窗口的结束

窗口的结束，计算公式如下：

~~~shell
窗口的结束 = 窗口的起始 + 窗口大小 - 1毫秒
~~~

演算如下：

~~~shell
#第一个窗口的结束
窗口的结束 = 窗口的起始 + 窗口大小 - 1毫秒
		 = 0 + 5 - 1毫秒
		 = 5 - 1毫秒
		 = 4999毫秒
窗口的结束时间是4999毫秒。
~~~

#### 窗口的排布

因为这是滚动窗口，所以，窗口与窗口之间是紧密排布的。数据是不重不丢。

~~~shell
#第一个窗口：
[0,5000)
[5000,10000)
[10000,15000)
...
~~~

#### 窗口的触发计算

窗口的触发计算 = 窗口的结束时间。

也就是说，窗口在结束那一刻开始，就触发窗口内的数据计算了。

#### SQL案例 - 扩展

建表和数据执行SQL语句一样，唯一不同就是数据的事件时间。

~~~shell
窗口的起始时间 = 第一条数据的事件时间 - （第一条数据的事件时间 % 窗口大小）
			= 1691933777 - （1691933777 % 5）
			= 1691933777 - （2）
			= 1691933775
这个时间就是窗口的起始时间。

结束时间：1691933775 + 5 - 1毫秒 = 1691933780 -（1毫秒） = 1691933779999毫秒
所以，当1691933780事件时间的数据到来时，会触发上一个窗口的计算。
~~~

截图如下：

![1691934633554](assets/1691934633554.png)

#### DataStream案例

##### 需求

~~~shell
演示基于事件时间的滚动窗口，数据源来自于socket(id,price,ts)，类型为：String,Integer,Long。
~~~

##### 分析









