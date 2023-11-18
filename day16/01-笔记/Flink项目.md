# Flink项目

## 今晚课程内容介绍

* 演示
* 数据模型
    * 聚合
    * 唯一
    * 冗余
* 数据导入
    * 本地文件
    * 分布式文件
    * Kafka数据源
    * insert into
* 数据导出
    * HDFS
* 数据删除
* 常规操作
* Rollup
* 物化视图
* 动态分区

## 演示

### 分区演示

查看分区命令：

~~~shell
#1.语法
show partitions from table_name;

#2.案例
show partitions from example_tb1;
~~~

#### range范围分区

~~~shell
CREATE TABLE IF NOT EXISTS test_db.example_tb1
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" 
	COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
    PARTITION `p202001` VALUES LESS THAN ("2020-02-01"),
    PARTITION `p202002` VALUES LESS THAN ("2020-03-01"),
    PARTITION `p202003` VALUES LESS THAN ("2020-04-01")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "1"
);
~~~

截图如下：

![1693914904023](assets/1693914904023.png)

#### list列表分区

~~~shell
CREATE TABLE IF NOT EXISTS test_db.example_list_tb2
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
    `city` VARCHAR(20) NOT NULL COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" 
	COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY LIST(`city`)
(
    PARTITION `p_cn` VALUES IN ("Beijing", "Shanghai", "Hong Kong"),
    PARTITION `p_usa` VALUES IN ("New York", "San Francisco"),
    PARTITION `p_jp` VALUES IN ("Tokyo")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "1"
);
~~~

截图如下：

![1693914884841](assets/1693914884841.png)

### 新增分区

#### range分区

~~~shell
ALTER TABLE test_db.example_tb1 ADD PARTITION IF NOT EXISTS `p202005` VALUES LESS THAN ("2020-06-01");
~~~

截图如下：

![1693915095472](assets/1693915095472.png)

#### list分区

~~~shell
ALTER TABLE test_db.example_list_tb2 ADD PARTITION IF NOT EXISTS p_uk VALUES IN ("London");
~~~

截图如下：

![1693915252420](assets/1693915252420.png)

### 删除分区

#### range分区

~~~shell
ALTER TABLE test_db.example_tb1 DROP PARTITION IF EXISTS p202003;
~~~

截图如下：

![1693915151539](assets/1693915151539.png)

#### list分区

~~~shell
ALTER TABLE test_db.example_list_tb2 DROP PARTITION IF EXISTS p_usa;
~~~

截图如下：

![1693915316757](assets/1693915316757.png)

### 单分区演示

单分区：不写分区，就是单分区，系统默认给定。

~~~shell
CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
~~~

截图如下：

![1693915433094](assets/1693915433094.png)

## 数据模型

Doris有三种数据模型：

* 聚合模型
* 唯一模型
* 冗余模型

### 聚合模型

聚合模型，Aggregate Key，相同的key，value会做聚合操作。按照给定的聚合函数进行聚合。

聚合函数有四种：

~~~shell
#1.sum
求和

#2.replace
替换

#3.max
求最大

#4.min
求最小
~~~

演示如下：

~~~shell
#1.创建表
CREATE TABLE IF NOT EXISTS test_db.example_site_visit
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES("replication_num" = "1");


#2.插入数据
insert into example_site_visit values(10000,'2017-10-01','北京',20,0,'2017-10-01 06:00:00',20,10,2);
insert into example_site_visit values(10000,'2017-10-01','北京',20,0,'2017-10-01 07:00:00',15,8,5);
insert into example_site_visit values(10001,'2017-10-01','北京',30,1,'2017-10-01 17:05:45',2,22,22);
insert into example_site_visit values(10002,'2017-10-02','上海',20,1,'2017-10-02 12:59:12',200,5,5);
insert into example_site_visit values(10003,'2017-10-02','广州',32,0,'2017-10-02 11:20:00',30,11,11);
insert into example_site_visit values(10004,'2017-10-01','深圳',35,0,'2017-10-01 10:00:15',100,3,3);
insert into example_site_visit values(10004,'2017-10-03','深圳',35,0,'2017-10-03 10:20:22',11,6,6);
~~~

截图如下：

![1693916147978](assets/1693916147978.png)

### 唯一模型

唯一模型，Unique Key，保证key列的唯一性。只要key相同，新的值会覆盖旧的值。

案例：

~~~shell
#1.创建表
CREATE TABLE IF NOT EXISTS test_db.user
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `phone` LARGEINT COMMENT "用户电话",
    `address` VARCHAR(500) COMMENT "用户地址",
    `register_time` DATETIME COMMENT "用户注册时间"
)
UNIQUE KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES("replication_num" = "1");



#2.插入数据
insert into user values(10000,'zhangsan','北京',20,0,13112345312,'北京西城区','2020-10-01 07:00:00');
insert into user values(10000,'zhangsan','深圳',20,0,13112345312,'深圳市宝安区','2020-11-15 06:10:20');
insert into user values(10001,'lisi','上海',20,0,13112345312,'上海市浦东区','2020-11-15 06:10:20');
insert into user values(10002,'lisi','广州',20,0,13112345312,'广州市天河区','2020-11-15 06:10:20');
~~~

唯一是聚合模型的特例，上述的建表等价于下面的建表：

~~~shell
CREATE TABLE IF NOT EXISTS test_db.user
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
    `city` VARCHAR(20) replace COMMENT "用户所在城市",
    `age` SMALLINT replace COMMENT "用户年龄",
    `sex` TINYINT replace COMMENT "用户性别",
    `phone` LARGEINT replace COMMENT "用户电话",
    `address` VARCHAR(500) replace COMMENT "用户地址",
    `register_time` DATETIME replace COMMENT "用户注册时间"
)
Aggregate KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES("replication_num" = "1");
~~~

### 冗余模型

冗余模型，Duplicate Key，运行数据冗余存储，保留数据原始的样子，不会对数据做任何操作。

案例：

~~~shell
#1.创建表
CREATE TABLE IF NOT EXISTS test_db.example_log
(
    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
    `type` INT NOT NULL COMMENT "日志类型",
    `error_code` INT COMMENT "错误码",
    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
    `op_id` BIGINT COMMENT "负责人id",
    `op_time` DATETIME COMMENT "处理时间"
)
DUPLICATE KEY(`timestamp`, `type`)
DISTRIBUTED BY HASH(`timestamp`) BUCKETS 10
PROPERTIES("replication_num" = "1");



#2.插入数据
insert into example_log values('2020-10-01 08:00:05',1,404,'not found page', 101,'2020-10-01 08:00:05');
insert into example_log values('2020-10-01 08:00:05',1,404,'not found page', 101,'2020-10-01 08:00:05');
insert into example_log values('2020-10-01 08:00:05',2,404,'not found page', 101,'2020-10-01 08:00:06');
insert into example_log values('2020-10-01 08:00:06',2,404,'not found page', 101,'2020-10-01 08:00:07');
~~~

截图如下：

![1693916823437](assets/1693916823437.png)

### 小结

这三种数据模型，各有各的特点，他们的适用场景也不太一样。

聚合模型：适用于有固定报表的场景。

唯一模型：适用于有主键（唯一列）的场景。比如数据库的数据。

冗余模型：如果没有其他模型适合，则可以使用冗余模型，它的使用场景不固定，不受模型约束，非常灵活。

建表时，可以省略，默认是冗余模型。

## 数据导入

Doris支持多种数据导入方式。常用的有如下几种：

* Stream Load（本地文件）
* Broker Load（HDFS）
* Routine Load（Kafka）
* Insert Into（MySQL的方式）

### Stream Load

Stream Load，流式导入，把本地的文件系统的数据导入到Doris中。

~~~shell
#1.创建表
CREATE TABLE test_db.user_result(
id BIGINT,
name VARCHAR(50),
age INT,
gender INT,
province  VARCHAR(50),
city   VARCHAR(50),
region  VARCHAR(50),
phone VARCHAR(50),
birthday VARCHAR(50),
hobby  VARCHAR(50),
register_date VARCHAR(50)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES("replication_num" = "1");


#2.导入语法
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load


#3.导入命令
curl --location-trusted -u root:123456 -H "column_separator:,"  -T /export/data/doris/user.csv -X PUT http://node1:8030/api/test_db/user_result/_stream_load


#4.命令介绍
http://fe_host:http_port：fe节点的主机名和端口号
{db}：数据库的名字
{table}：表的名称
data.file：数据文件，以防出错，可以使用全路径。
~~~

截图如下：

![1693918960155](assets/1693918960155.png)

### Broker Load

Broker导入，也就是说，需要使用到Broker进程。所以前提是要先启动Broker进程。

Broker导入，主要用于和分布式文件系统打交道。也就是把分布式文件系统的数据导入到Doris中。

#### 前提

启动HDFS和Broker。

~~~shell
#1.启动HDFS
start-dfs.sh

#2.启动Broker
cd /export/server/doris
apache_hdfs_broker/bin/start_broker.sh --daemon
~~~

#### 准备数据

~~~shell
#1.创建目录
hdfs dfs -mkdir -p /datas/doris 

#2.上传数据
hdfs dfs -put /export/data/doris/user.csv /datas/doris
~~~

#### 创建表

~~~shell
#1.表已经在前面创建好了，这里需要清空表才行。
truncate table user_result;
~~~

#### 创建导入任务

~~~shell
LOAD LABEL test_db.user_result
(
DATA INFILE("hdfs://node1:8020/datas/doris/user.csv")
INTO TABLE `user_result`
COLUMNS TERMINATED BY ","
FORMAT AS "csv"
(id, name, age, gender, province,city,region,phone,birthday,hobby,register_date)
)
WITH BROKER broker_name
(
"dfs.nameservices" = "my_cluster",
"dfs.ha.namenodes.my_cluster" = "nn1",
"dfs.namenode.rpc-address.my_cluster.nn1" = "node1:8020",
"dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)
PROPERTIES
(
    "max_filter_ratio"="0.00002"
);


#2.查看任务状态
show load\G
~~~

截图如下：

![1693919888356](assets/1693919888356.png)

### Routine Load

可以导入流数据，比如Kafka。目前仅支持Kafka数据源。

#### 启动Kafka

~~~shell
#1.启动Zookeeper
zkServer.sh start

#2.启动kafka
cd /export/server/kafka
nohup bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log &

#3.连接Kafka，使用Kafka Tool
bin/kafka-topics.sh --create --topic test --bootstrap-server node1:9092

#4.启动Kafka的生产者
 bin/kafka-console-producer.sh --broker-list node1:9092 --topic test

#5.发送数据
{"id":1,"name":"zhangsan","age":30}
{"id":2,"name":"lisi","age":18}
{"id":3,"name":"wangwu","age":19}
~~~

#### 创建Doris的表

~~~shell
create table student_kafka
(
id int,
name varchar(50),
age int
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES("replication_num" = "1");
~~~

#### 创建Routine Load任务

~~~shell
#1.创建表
CREATE ROUTINE LOAD test_db.kafka_job1 on student_kafka
PROPERTIES
(
    "desired_concurrent_number"="1",
"strict_mode"="false",
    "format" = "json"
)
FROM KAFKA
(
    "kafka_broker_list"= "node1:9092",
    "kafka_topic" = "test",
    "property.group.id" = "test_group_1",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    "property.enable.auto.commit" = "false"
);


#2.查看任务命令
show routine load;


#3.停止任务
stop routine load for kafka_job1;


#4.查看任务，这里要加上all关键字
show all routine load;
~~~

截图如下：

![1693921036886](assets/1693921036886.png)

注意事项：任务一旦停止，则数据就无法从Kafka中导入到Doris中了。

### Insert Into

和MySQL的插入语句是一样的，略。

## 数据导出

Doris支持数据从表中到处到HDFS上。

~~~shell
#1.帮助命令
help export;


#2.创建导出任务
EXPORT TABLE test_db.example_site_visit 
TO "hdfs://node1:8020/datas/output1" 
WITH BROKER "broker_name" (
"username"="root", 
"password"="123456"
);
~~~

截图如下：

![1693921574034](assets/1693921574034.png)

## 数据删除

Doris支持数据删除。可以从两个层面来删除。

* delete删除一条
* drop partition删除一个分区

### delete删除

~~~shell
delete from example_visit_site where id = 10003；
~~~

截图如下：

![1693921779241](assets/1693921779241.png)

### 分区删除

~~~shell
alter table 表名 drop partition 分区名;
~~~

案例：

~~~shell
#1.创建表
CREATE TABLE table2
(
    event_day DATE,
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, siteid, citycode, username)
PARTITION BY RANGE(event_day)
(
    PARTITION p202006 VALUES LESS THAN ('2020-07-01'),
    PARTITION p202007 VALUES LESS THAN ('2020-08-01'),
    PARTITION p202008 VALUES LESS THAN ('2020-09-01')
)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");


#2.导入数据
cd /export/data/doris
curl --location-trusted -u root:123456  -H "column_separator:|" -T table2_data http://node1:8030/api/test_db/table2/_stream_load


#3.查看数据
select * from table2;


#4.查看分区
show partitions from table2;


#5.删除分区
alter table table2 drop partition p202007;

#6.查看数据
select * from table2;
~~~

截图如下：

![1693921988446](assets/1693921988446.png)

## Rollup&物化视图

### MySQL的索引

索引的目的：加快查询。

索引的分类：

~~~shell
#1.主键索引
主键列，自动带主键索引，这个索引是MySQL自动创建的

#2.唯一索引
唯一列，对表中唯一列的数据，可以创建唯一索引

#3.普通索引（单列）
对普通的列创建的索引（name）

#4.聚合索引（组合、多列）
对普通的多个列创建的索引（name+age）
查询的时候，必须以name打头才能匹配到聚合索引，如果以age打头，则没办法匹配这个聚合索引。
~~~

### Oracle的物化视图

视图：保存了计算逻辑，但是不存储数据。

物化视图：物理化存储。把视图的数据物理化存储起来。单独存储。

MySQL中没有物化视图，Oracle中有。Hive的3.x版本中也有物化视图。

### Rollup

#### 概述

Rollup = MySQL的索引+Oracle的物化视图。

Rollup：可以人为调整列的顺序，增加前缀索引的匹配度，提升查询效率。

Doris建表默认是有顺序的，这个顺序就是字段的顺序，可以任务这就是它默认的聚合索引。

比如Doris的表：

~~~shell
event_day
citycode
siteid
username
~~~

如果希望以username来检索数据，则就会非常慢，因为没有索引可以匹配。

怎么办呢？

能不能自己创建Doris的索引呢？

可以。

这就是Rollup。但是Rollup又不局限于索引，它还可以额外保存索引的数据。

也就是说，Rollup可以保存以索引创建的数据文件，如果查询数据时，则可以单独查询这个文件，而不需要再去查询源表了。

#### 演示

查看表的Rollup：

~~~shell
desc table_name all;
~~~

创建Rollup语法：

~~~shell
alter table table_name add rollup rollup_name ();
~~~

~~~shell
user_id        
date           
city           
age            
sex            
last_visit_date
cost           
max_dwell_time 
min_dwell_time 

#1.创建rollup
alter table example_site_visit add rollup rollup_cost_userid(user_id,cost);


#2.再创建rollup
alter table example_site_visit add rollup rollup_cost_userid2(age,date,city,user_id,sex,last_visit_date,cost,max_dwell_time,min_dwell_time);


#3.查询条件中带上age
EXPLAIN SELECT * FROM example_site_visit where age=20 and city LIKE "%上%";
~~~

#### 不同数据模型下的rollup

聚合模型：rollup有聚合的能力，也有调整列的顺序能力

唯一模型：rollup有聚合的能力，也有调整列的顺序能力

冗余模型：只有调整列的顺序能力

rollup是根据数据模型而来的。是什么模型，模型有什么能力，rollup就有什么能力。

#### 物化视图

Doris的物化视图，本质上就是一个rollup。

~~~shell
#1.创建表
CREATE TABLE sales_records(
	id INT COMMENT "销售记录的id",
	seller_id INT COMMENT "销售员的id",
	store_id  INT COMMENT "门店的id",
	sale_date DATE COMMENT "售卖时间",
	sale_amt BIGINT COMMENT "金额"
)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES( "replication_num" = "1" );


#2.创建物化视图
CREATE MATERIALIZED VIEW mv_1 AS
SELECT seller_id,sale_date,SUM(sale_amt)
FROM sales_records
GROUP BY seller_id,sale_date;
~~~

## 动态分区

动态分区，区别于之前的手动分区。

手动分区：自己管理分区。

动态分区：Doris管理。自己不需要管理。

Doris支持四种动态分区的方式：

* hour
* day
* week
* month

动态分区需要开启开关：

~~~shell
#1.开启动态分区
curl --location-trusted -u root:123456 -XGET http://node1:8030/api/_set_config?dynamic_partition_enable=true

#2.设置动态分区的检测时间间隔
curl --location-trusted -u root:123456 -XGET http://node1:8030/api/_set_config?dynamic_partition_check_interval_seconds=5
~~~

动态分区的语法：

~~~shell
#1.语法
PARTITION BY RANGE('分区字段')()

#2.注意
不支持list分区。
~~~

### day

~~~shell
#1.创建表
CREATE TABLE order_dynamic_partition1
(
id int,
time date,
money double,
areaName varchar(50)
)
duplicate key(id,time)
PARTITION BY RANGE(time)()
DISTRIBUTED BY HASH(id) buckets 10
PROPERTIES(
	"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-7",
      "dynamic_partition.end" = "3",
      "dynamic_partition.prefix" = "p",
      "dynamic_partition.buckets" = "10",
	"replication_num" = "1"
);


#2.查看分区
show partitions from order_dynamic_partition1;
~~~

截图如下：

![1694091318341](assets/1694091318341.png)

### week

~~~shell
#1.创建表
CREATE TABLE order_dynamic_partition2
(
id int,
time date,
money double,
areaName varchar(50)
)
duplicate key(id,time)
PARTITION BY RANGE(time)()
DISTRIBUTED BY HASH(id) buckets 10
PROPERTIES(
	"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "WEEK",
"dynamic_partition.start" = "-7",
      "dynamic_partition.end" = "3",
      "dynamic_partition.prefix" = "p",
      "dynamic_partition.buckets" = "10",
	"replication_num" = "1"
);


#2.查看分区
show partitions from order_dynamic_partition2;
~~~

截图如下：

![1694091446883](assets/1694091446883.png)

### 动态分区表和手动分区表之间的转换

#### 普通表转换为动态分区表

~~~shell
#1.创建表
CREATE TABLE table_partition
(
id int,
time date,
money double,
areaName varchar(50)
)
duplicate key(id,time)
PARTITION BY RANGE(time)
(
    PARTITION `p202001` VALUES LESS THAN ("2020-02-01"),
    PARTITION `p202002` VALUES LESS THAN ("2020-03-01"),
PARTITION `p202003` VALUES LESS THAN ("2020-04-01")
)
DISTRIBUTED BY HASH(id) buckets 10
PROPERTIES
(
   "dynamic_partition.enable" = "false",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.prefix" = "p",
"dynamic_partition.end" = "3",
"dynamic_partition.buckets" = "10",
"replication_num" = "1"
);


#2.查看哪些表是动态分区表
show dynamic partition tables;


#3.alter表就可以转换为动态分区
ALTER TABLE table_partition set (
"dynamic_partition.enable" = "true",
"dynamic_partition.start" = "-1", 
"dynamic_partition.end" = "3"
);
~~~

截图如下：

![1694091796942](assets/1694091796942.png)

#### 动态分区表转换为普通分区表

~~~shell
ALTER TABLE order_dynamic_partition1 set (
"dynamic_partition.enable" = "false");
~~~

截图如下：

![1694091875976](assets/1694091875976.png)

## 内置函数

~~~shell
#1.语法
show builtin functions in database_name;

#2.使用
show builtin functions in test_db;

#3.具体函数的使用方式
help + 函数名;

#4.如果上述查不到使用方式，则去官网查看。
https://doris.apache.org/docs/dev/sql-manual/sql-functions/array-functions/array
~~~

## 综合案例

### FlinkSQL-Doris

#### 需求

~~~shell
通过FlinkSQL客户端，把数据Sink到Doris中
~~~

#### 分析

![1694092283776](assets/1694092283776.png)

操作步骤：

~~~shell
#1.在Doris中创建库、表
#2.在FlinkSQL客户端中创建Doris物理表的映射表（sink_table）
#3.插入数据
	insert into sink_table values ...
#4.校验数据（Doris）
~~~

#### 启动服务

~~~shell
#1.启动Flink
start-cluster.sh


#2.启动Doris
cd /export/server/doris
fe/bin/start_fe.sh --daemon
be/bin/start_be.sh --daemon


#3.启动HDFS
start-dfs.sh


#4.进入FlinkSQL客户端
sql-client.sh
~~~

#### 实现

##### 在Doris中创建库、表

~~~shell
#1.创建数据库
create database test;

#2.切换库
use test;

#3.创建表
CREATE TABLE if not exists test.demo
(
    id    int,
    name STRING,
    age   INT,
    price DECIMAL(5, 2),
    sale  DOUBLE
) UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

##### 在FlinkSQL客户端中创建Doris物理表的映射表

~~~shell
#规则：字段名一样，类型匹配上，表名随意
CREATE TABLE flink_doris_sink
(
    id    int,
    name STRING,
    age   INT,
    price DECIMAL(5, 2),
    sale  DOUBLE,
    PRIMARY KEY (`id`) NOT ENFORCED
)
WITH (
    'connector' = 'doris'
    ,'fenodes' = 'node1:8030'
    ,'password' = '123456'
    ,'username' = 'root'
    ,'table.identifier' = 'test.demo'
    ,'sink.enable-delete' = 'true'
    ,'sink.properties.strip_outer_array' = 'true'
    ,'sink.batch.size' = '2000'
    ,'sink.batch.interval' = '10s'
    ,'sink.properties.format' = 'json'
);


#官网链接
https://doris.apache.org/docs/dev/ecosystem/flink-doris-connector
~~~

##### 插入数据

~~~shell
insert into flink_doris_sink values(1,'zhangsan',30,6.66,5);
~~~

##### 校验数据（Doris）

![1694093466373](assets/1694093466373.png)

### FlinkCDC-Doris

#### 需求

~~~shell
通过FlinkCDC把MySQL的数据同步到Doris中
~~~

#### 分析

![1694094049782](assets/1694094049782.png)

操作步骤：

~~~shell
在MySQL中准备库、表，插入数据
在Doris中准备库、表，用来接受MySQL的数据
在FlinkSQL创建MySQL的映射表（source_table）
在FlinkSQL创建Doris的映射表（sink_table）
拉起数据任务
	insert into sink_table select * from source_table
校验数据（Doris）
~~~

#### 实现

##### 在MySQL中准备库、表，插入数据

~~~shell
#1.创建库
create database doris_testdb;

#2.切换库
use doris_testdb;

#3.创建表
CREATE TABLE if not exists doris_testdb.demo
(
    id    int,
    name  varchar(255),
    age   INT,
    price DECIMAL(5, 2),
    sale  DOUBLE,
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci;


#4.插入数据
insert into demo values(1,'zhangsan',30,6.66,5);
insert into demo values(2,'lisi',18,18.88,66);
insert into demo values(3,'wangwu',25,188,1);
~~~

##### 在Doris中准备库、表，用来接受MySQL的数据

~~~shell
CREATE TABLE if not exists test.demo2
(
    id    int,
    name STRING,
    age   INT,
    price DECIMAL(5, 2),
    sale  DOUBLE
) UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

##### 在FlinkSQL创建MySQL的映射表（source_table）

~~~shell
CREATE TABLE flink_doris_source (
    id int,
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE,
    PRIMARY KEY ( `id` ) NOT ENFORCED
) WITH (
    'connector'= 'mysql-cdc',
    'hostname'= 'node1',
    'port'= '3306',
    'username'= 'root',
    'password'='123456',
    'server-time-zone'= 'Asia/Shanghai',
    'debezium.snapshot.mode'='initial',
    'database-name'= 'doris_testdb',
    'table-name'= 'demo'
);
~~~

##### 在FlinkSQL创建Doris的映射表（sink_table）

~~~shell
CREATE TABLE flink_doris_sink2 (
    id int,
    name STRING,
    age INT,
    price DECIMAL(5,2),
    sale DOUBLE,
    PRIMARY KEY ( `id` ) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'test.demo2'
    ,'sink.enable-delete' = 'true'
    ,'sink.properties.strip_outer_array' = 'true'
    ,'sink.batch.size' = '2000'
    ,'password' = '123456'
    ,'connector' = 'doris'
    ,'sink.batch.interval' = '10s'
    ,'sink.max-retries' = '5'
    ,'sink.properties.format' = 'json'
    ,'username' = 'root'
);
~~~

##### 拉起数据任务

~~~shell
INSERT INTO flink_doris_sink2 select id,name,age,price,sale from flink_doris_source;
~~~

##### 校验数据（Doris）

![1694094846489](assets/1694094846489.png)

### 官网笔记

#### 版本兼容

| Connector Version | Flink Version  | Doris Version | Java Version | Scala Version |
| ----------------- | -------------- | ------------- | ------------ | ------------- |
| 1.0.3             | 1.11+          | 0.15+         | 8            | 2.11,2.12     |
| 1.1.1             | 1.14           | 1.0+          | 8            | 2.11,2.12     |
| 1.2.1             | 1.15           | 1.0+          | 8            | -             |
| 1.3.0             | 1.16           | 1.0+          | 8            | -             |
| 1.4.0             | 1.15,1.16,1.17 | 1.0+          | 8            | -             |

#### 建表参数

##### 通用参数

| Key                              | Default Value | Required | Comment                                                      |
| -------------------------------- | ------------- | -------- | ------------------------------------------------------------ |
| fenodes                          | --            | Y        | Doris FE http address, multiple addresses are supported, separated by commas |
| benodes                          | --            | N        | Doris BE http address, multiple addresses are supported, separated by commas. refer to [#187](https://github.com/apache/doris-flink-connector/pull/187) |
| table.identifier                 | --            | Y        | Doris table name, such as: db.tbl                            |
| username                         | --            | Y        | username to access Doris                                     |
| password                         | --            | Y        | Password to access Doris                                     |
| doris.request.retries            | 3             | N        | Number of retries to send requests to Doris                  |
| doris.request.connect.timeout.ms | 30000         | N        | Connection timeout for sending requests to Doris             |
| doris.request.read.timeout.ms    | 30000         | N        | Read timeout for sending requests to Doris                   |

##### source参数

| Key                           | Default Value      | Required | Comment                                                      |
| ----------------------------- | ------------------ | -------- | ------------------------------------------------------------ |
| doris.request.query.timeout.s | 3600               | N        | The timeout time for querying Doris, the default value is 1 hour, -1 means no timeout limit |
| doris.request.tablet.size     | Integer. MAX_VALUE | N        | The number of Doris Tablets corresponding to a Partition. The smaller this value is set, the more Partitions will be generated. This improves the parallelism on the Flink side, but at the same time puts more pressure on Doris. |
| doris.batch.size              | 1024               | N        | The maximum number of rows to read data from BE at a time. Increasing this value reduces the number of connections established between Flink and Doris. Thereby reducing the additional time overhead caused by network delay. |
| doris.exec.mem.limit          | 2147483648         | N        | Memory limit for a single query. The default is 2GB, in bytes |
| doris.deserialize.arrow.async | FALSE              | N        | Whether to support asynchronous conversion of Arrow format to RowBatch needed for flink-doris-connector iterations |
| doris.deserialize.queue.size  | 64                 | N        | Asynchronous conversion of internal processing queue in Arrow format, effective when doris.deserialize.arrow.async is true |
| doris.read.field              | --                 | N        | Read the list of column names of the Doris table, separated by commas |
| doris.filter.query            | --                 | N        | The expression to filter the read data, this expression is transparently passed to Doris. Doris uses this expression to complete source-side data filtering. For example age=18. |

##### sink参数

| Key                | Default Value | Required | Comment                                                      |
| ------------------ | ------------- | -------- | ------------------------------------------------------------ |
| sink.label-prefix  | --            | Y        | The label prefix used by Stream load import. In the 2pc scenario, global uniqueness is required to ensure Flink's EOS semantics. |
| sink.properties.*  | --            | N        | Import parameters for Stream Load. For example: 'sink.properties.column_separator' = ', ' defines column delimiters, 'sink.properties.escape_delimiters' = 'true' special characters as delimiters, '\x01' will be converted to binary 0x01  JSON format import 'sink.properties.format' = 'json' 'sink.properties. read_json_by_line' = 'true' Detailed parameters refer to [here](https://doris.apache.org/docs/dev/data-operate/import/import-way/stream-load-manual). |
| sink.enable-delete | TRUE          | N        | Whether to enable delete. This option requires the Doris table to enable the batch delete function (Doris 0.15+ version is enabled by default), and only supports the Unique model. |
| sink.enable-2pc    | TRUE          | N        | Whether to enable two-phase commit (2pc), the default is true, to ensure Exactly-Once semantics. For two-phase commit, please refer to [here](https://doris.apache.org/docs/dev/data-operate/import/import-way/stream-load-manual). |
| sink.buffer-size   | 1MB           | N        | The size of the write data cache buffer, in bytes. It is not recommended to modify, the default configuration is enough |
| sink.buffer-count  | 3             | N        | The number of write data buffers. It is not recommended to modify, the default configuration is enough |
| sink.max-retries   | 3             | N        | Maximum number of retries after Commit failure, default 3    |

#### 字段映射

| Doris Type | Flink Type           |
| ---------- | -------------------- |
| NULL_TYPE  | NULL                 |
| BOOLEAN    | BOOLEAN              |
| TINYINT    | TINYINT              |
| SMALLINT   | SMALLINT             |
| INT        | INT                  |
| BIGINT     | BIGINT               |
| FLOAT      | FLOAT                |
| DOUBLE     | DOUBLE               |
| DATE       | DATE                 |
| DATETIME   | TIMESTAMP            |
| DECIMAL    | DECIMAL              |
| CHAR       | STRING               |
| LARGEINT   | STRING               |
| VARCHAR    | STRING               |
| DECIMALV2  | DECIMAL              |
| TIME       | DOUBLE               |
| HLL        | Unsupported datatype |



