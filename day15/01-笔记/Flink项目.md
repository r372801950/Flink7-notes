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

## Rollup

## 物化视图

## 动态分区

## 内置函数

## 综合案例







