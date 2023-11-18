# Flink项目

## 今晚课程内容介绍

* CDC
* FlinkCDC
* FlinkCDC Connector
* FlinkCDC入门案例
* 数据湖

## CDC

### 概述

CDC，Change Data Capture，翻译为变更数据捕获。简称为CDC。包含两种含义：

* 广义上，指的是任意数据的变更，比如文件、视频、音频、图片等，只要能够捕获住这种变化，我们就称之为CDC。
* 狭义上，专门指的**数据库**的变更。也就是说，但凡是能够捕获住数据库的数据变更，我们就称之为CDC。

说明：后续我们说的CDC就是指**狭义上**的。

数据库中的数据变更：指的是insert、update、delete这些操作。

结论：CDC是一种思想，不涉及某一门具体的计算框架。

### CDC的实现机制

#### 主动查询

每隔一段时间，就主动查询一次数据库，以此来获得数据库的变更数据。

优点：

能够节省资源。

能够完成需求。

缺点：

延迟较高。

给数据库带来额外的访问压力。

Sqoop就是基于这种方式来实现的。

#### 基于日志

前提：开启数据库的二进制日志开关。默认开关是关闭的。

开启开关后，对于数据库级别的数据变更，数据库自身就会纪录二进制日志。我们只需要监听二进制日志文件，即可获取数据库级别的数据变更。

优点：

延迟较低。

不会给数据库带来额外的访问压力。

缺点：

占用服务器存储资源。

Canal就是基于这种方式来实现的。

### CDC工具

围绕着CDC的思想下，业界常用的CDC工具如下图：

![1693310763305](assets/1693310763305.png)

从图上可以看出，FlinkCDC是CDC的一种实现而已。除了FlinkCDC之外，CDC还有其他的一些实现方式，比如：Sqoop、Canal等。

## FlinkCDC

### 概述

它是一个数据库的实时数据同步工具。能顾支撑全量+增量的同步。

官网：https://ververica.github.io/flink-cdc-connectors/

### 历史

2020年7月份，由云邪（花名）因为个人兴趣研发，目的是为了扩展Flink的生态。

在2020年7月支持了MySQL数据库。在7月底开始支持PostgreSQL。

2021年7月份，推出了FlinkCDC2.0版本。

现在最新版为2.4版本。

项目中用的2.2版本，该版本于2022年发布。

### 特点

* 精准一次语义
* 支持SQL/Table API
* 支持DataStream API

### 架构

![img](https://ververica.github.io/flink-cdc-connectors/assets/cdc-flow.png)

同步数据的方式有两种：

* SQL

* DataStream

项目中我们使用的是SQL。官网也推荐SQL。

### FlinkCDC Connector

官网的连接器如下：

| Connector                                                    | Database                                                     | Driver                    |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------- |
| [mongodb-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html) | [MongoDB](https://www.mongodb.com/): 3.6, 4.x, 5.0           | MongoDB Driver: 4.3.4     |
| [mysql-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html) | [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x[RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x[PolarDB MySQL](https://www.aliyun.com/product/polardb): 5.6, 5.7, 8.0.x[Aurora MySQL](https://aws.amazon.com/cn/rds/aurora): 5.6, 5.7, 8.0.x[MariaDB](https://mariadb.org/): 10.x[PolarDB X](https://github.com/ApsaraDB/galaxysql): 2.0.1 | JDBC Driver: 8.0.28       |
| [oceanbase-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oceanbase-cdc.html) | [OceanBase CE](https://open.oceanbase.com/): 3.1.x, 4.x[OceanBase EE](https://www.oceanbase.com/product/oceanbase): 2.x, 3.x, 4.x | OceanBase Driver: 2.4.x   |
| [oracle-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html) | [Oracle](https://www.oracle.com/index.html): 11, 12, 19, 21  | Oracle Driver: 19.3.0.0   |
| [postgres-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html) | [PostgreSQL](https://www.postgresql.org/): 9.6, 10, 11, 12, 13, 14 | JDBC Driver: 42.5.1       |
| [sqlserver-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/sqlserver-cdc.html) | [Sqlserver](https://www.microsoft.com/sql-server): 2012, 2014, 2016, 2017, 2019 | JDBC Driver: 9.4.1.jre8   |
| [tidb-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/tidb-cdc.html) | [TiDB](https://www.pingcap.com/): 5.1.x, 5.2.x, 5.3.x, 5.4.x, 6.0.0 | JDBC Driver: 8.0.27       |
| [db2-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/db2-cdc.html) | [Db2](https://www.ibm.com/products/db2): 11.5                | Db2 Driver: 11.5.0.0      |
| [vitess-cdc](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/vitess-cdc.html) | [Vitess](https://vitess.io/): 8.0.x, 9.0.x                   | MySql JDBC Driver: 8.0.26 |

命名规则：数据库-cdc

比如：MySQL数据库，我们可以使用传统的JDBC的Connector去连接。

也可以使用F林可CDC的连接器。

比如：

~~~shell
#1.使用jdbc 连接器
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://node1:3306/test',
   'table-name' = 'users'
   'username' = 'root',
   'password' = '123456'
);

#2.使用flink-cdc 连接器
CREATE TABLE mysql_binlog (
  id INT NOT NULL,
  name STRING,
  description STRING,
  weight DECIMAL(10,3),
  PRIMARY KEY(id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'node1',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'test',
  'table-name' = 'users'
);
~~~

### FlinkCDC Connector VS JDBC Connector

* 名字不同。FlinkCDC的连接器的名字和数据库相关，JDBC连接器的名字是固定的
* 读取方式不同。FlinkCDC是持续监听数据库的二级制日志的，JDBC连接器是针对数据库表操作的，一般是CURD语句。是瞬时的。

### FlinkCDC和Flink的版本兼容

| Flink® CDC Version | Flink® Version                         |
| ------------------ | -------------------------------------- |
| 1.0.0              | 1.11.*                                 |
| 1.1.0              | 1.11.*                                 |
| 1.2.0              | 1.12.*                                 |
| 1.3.0              | 1.12.*                                 |
| 1.4.0              | 1.13.*                                 |
| 2.0.*              | 1.13.*                                 |
| 2.1.*              | 1.13.*                                 |
| 2.2.*              | 1.13.*, 1.14.*                         |
| 2.3.*              | 1.13.*, 1.14.*, 1.15.*, 1.16.0         |
| 2.4.*              | 1.13.*, 1.14.*, 1.15.*, 1.16.*, 1.17.0 |

## 入门案例

### 前提

开启MySQL的二进制日志（binlog），编辑`/etc/profile`文件，在`[mysqld]`标签下，添加如下内容：

~~~shell
[mysqld]
server_id=1
log_bin = mysql-bin
binlog_format = ROW
expire_logs_days = 30				#这个配置在公司中不要配，这里是虚拟机环境，可以配置。
~~~

如果是第一次配置，则重启MySQL数据库即可。

~~~shell
systemctl restart mysqld
~~~

### Flink整合FlinkCDC

从官网上下载预编译的jar包，并且把jar包放置在`FLINK_HOME/lib`目录下，重启Flink集群即可。

~~~shell
flink-sql-connector-mysql-cdc-2.2.1.jar
~~~

### 启动

~~~shell
#0.启动HDFS，Flink的Checkpoint配置的是Fs
start-dfs.sh

#1.启动Flink集群
start-cluster.sh

#2.进入FlinkSQL客户端
sql-client.sh
~~~

### 实操

#### MySQL的库表数据

~~~sql
--删除test数据库
Drop database if exists test;

--创建test数据库
create database test character set utf8;

--切换test数据库
Use test;


-- 建表
-- 学生表
CREATE TABLE `Student`(
      `s_id` VARCHAR(20),
      `s_name` VARCHAR(20) NOT NULL DEFAULT '',
      `s_birth` VARCHAR(20) NOT NULL DEFAULT '',
      `s_sex` VARCHAR(10) NOT NULL DEFAULT '',
      PRIMARY KEY(`s_id`)
);

-- 成绩表
CREATE TABLE `Score`(
    `s_id` VARCHAR(20),
    `c_id` VARCHAR(20),
    `s_score` INT(3),
    PRIMARY KEY(`s_id`,`c_id`)
);

-- 插入学生表测试数据
insert into Student values('01' , '赵雷' , '1990-01-01' , '男');
insert into Student values('02' , '钱电' , '1990-12-21' , '男');
insert into Student values('03' , '孙风' , '1990-05-20' , '男');
insert into Student values('04' , '李云' , '1990-08-06' , '男');
insert into Student values('05' , '周梅' , '1991-12-01' , '女');
insert into Student values('06' , '吴兰' , '1992-03-01' , '女');
insert into Student values('07' , '郑竹' , '1989-07-01' , '女');
insert into Student values('08' , '王菊' , '1990-01-20' , '女');

-- 成绩表测试数据
insert into Score values('01' , '01' , 80);
insert into Score values('01' , '02' , 90);
insert into Score values('01' , '03' , 99);
insert into Score values('02' , '01' , 70);
insert into Score values('02' , '02' , 60);
insert into Score values('02' , '03' , 80);
insert into Score values('03' , '01' , 80);
insert into Score values('03' , '02' , 80);
insert into Score values('03' , '03' , 80);
insert into Score values('04' , '01' , 50);
insert into Score values('04' , '02' , 30);
insert into Score values('04' , '03' , 20);
insert into Score values('05' , '01' , 76);
insert into Score values('05' , '02' , 87);
insert into Score values('06' , '01' , 31);
insert into Score values('06' , '03' , 34);
insert into Score values('07' , '02' , 89);
insert into Score values('07' , '03' , 98);
~~~

#### FlinkSQL建表

~~~sql
--建表规则
--1.字段名必须相同
--2.数据类型必须匹配上
--3.FlinkSQL中的表名无所谓，不需要和MySQL中的源表一样


--创建表
create table my_student (
s_id string primary key not enforced,
s_name string,
s_birth string,
s_sex string
) with (
'connector' = 'mysql-cdc',
'hostname' = 'node1',
'port' = '3306',
'username' = 'root',
'password' = '123456',
'database-name' = 'test',
'table-name' = 'Student',
'scan.startup.mode' = 'initial',
'server-time-zone' = 'Asia/Shanghai'
);


--执行查询SQL
select * from my_student;
~~~

#### 校验

手动对数据库中的表进行增加、删除、修改操作，校验FlinkCDC的时效性。

~~~sql
--新增操作
insert into Student values ('09' , '李云龙' , '1980-10-10' , '男');

--修改操作
update Student set s_name = '和尚' where s_id = '09';

--删除操作
delete from Student where s_id = '09';

--查询操作
select * from Student;
~~~

### mysql-cdc的参数

| Option                                     | Required | Default | Type     | Description                                                  |
| ------------------------------------------ | -------- | ------- | -------- | ------------------------------------------------------------ |
| connector                                  | required | (none)  | String   | 指定要使用的连接器, 这里应该是 `'mysql-cdc'`.                |
| hostname                                   | required | (none)  | String   | MySQL 数据库服务器的 IP 地址或主机名。                       |
| username                                   | required | (none)  | String   | 连接到 MySQL 数据库服务器时要使用的 MySQL 用户的名称。       |
| password                                   | required | (none)  | String   | 连接 MySQL 数据库服务器时使用的密码。                        |
| database-name                              | required | (none)  | String   | 要监视的 MySQL 服务器的数据库名称。数据库名称还支持正则表达式，以监视多个与正则表达式匹配的表。 |
| table-name                                 | required | (none)  | String   | 需要监视的 MySQL 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。注意：MySQL CDC 连接器在正则匹配表名时，会把用户填写的 database-name， table-name 通过字符串 `\\.` 连接成一个全路径的正则表达式，然后使用该正则表达式和 MySQL 数据库中表的全限定名进行正则匹配。 |
| port                                       | optional | 3306    | Integer  | MySQL 数据库服务器的整数端口号。                             |
| server-id                                  | optional | (none)  | String   | 读取数据使用的 server id，server id 可以是个整数或者一个整数范围，比如 '5400' 或 '5400-5408', 建议在 'scan.incremental.snapshot.enabled' 参数为启用时，配置成整数范围。因为在当前 MySQL 集群中运行的所有 slave 节点，标记每个 salve 节点的 id 都必须是唯一的。 所以当连接器加入 MySQL 集群作为另一个 slave 节点（并且具有唯一 id 的情况下），它就可以读取 binlog。 默认情况下，连接器会在 5400 和 6400 之间生成一个随机数，但是我们建议用户明确指定 Server id。 |
| scan.incremental.snapshot.enabled          | optional | true    | Boolean  | 增量快照是一种读取表快照的新机制，与旧的快照机制相比， 增量快照有许多优点，包括： （1）在快照读取期间，Source 支持并发读取， （2）在快照读取期间，Source 支持进行 chunk 粒度的 checkpoint， （3）在快照读取之前，Source 不需要数据库锁权限。 如果希望 Source 并行运行，则每个并行 Readers 都应该具有唯一的 Server id，所以 Server id 必须是类似 `5400-6400` 的范围，并且该范围必须大于并行度。 请查阅 [增量快照读取](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc%28ZH%29.html#a-name-id-001-a) 章节了解更多详细信息。 |
| scan.incremental.snapshot.chunk.size       | optional | 8096    | Integer  | 表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。 |
| scan.snapshot.fetch.size                   | optional | 1024    | Integer  | 读取表快照时每次读取数据的最大条数。                         |
| scan.startup.mode                          | optional | initial | String   | MySQL CDC 消费者可选的启动模式， 合法的模式为 "initial"，"earliest-offset"，"latest-offset"，"specific-offset" 和 "timestamp"。 请查阅 [启动模式](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc%28ZH%29.html#a-name-id-002-a) 章节了解更多详细信息。 |
| scan.startup.specific-offset.file          | optional | (none)  | String   | 在 "specific-offset" 启动模式下，启动位点的 binlog 文件名。  |
| scan.startup.specific-offset.pos           | optional | (none)  | Long     | 在 "specific-offset" 启动模式下，启动位点的 binlog 文件位置。 |
| scan.startup.specific-offset.gtid-set      | optional | (none)  | String   | 在 "specific-offset" 启动模式下，启动位点的 GTID 集合。      |
| scan.startup.specific-offset.skip-events   | optional | (none)  | Long     | 在指定的启动位点后需要跳过的事件数量。                       |
| scan.startup.specific-offset.skip-rows     | optional | (none)  | Long     | 在指定的启动位点后需要跳过的数据行数量。                     |
| server-time-zone                           | optional | (none)  | String   | 数据库服务器中的会话时区， 例如： "Asia/Shanghai". 它控制 MYSQL 中的时间戳类型如何转换为字符串。 更多请参考 [这里](https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types). 如果没有设置，则使用ZoneId.systemDefault()来确定服务器时区。 |
| debezium.min.row. count.to.stream.result   | optional | 1000    | Integer  | 在快照操作期间，连接器将查询每个包含的表，以生成该表中所有行的读取事件。 此参数确定 MySQL 连接是否将表的所有结果拉入内存（速度很快，但需要大量内存）， 或者结果是否需要流式传输（传输速度可能较慢，但适用于非常大的表）。 该值指定了在连接器对结果进行流式处理之前，表必须包含的最小行数，默认值为1000。将此参数设置为`0`以跳过所有表大小检查，并始终在快照期间对所有结果进行流式处理。 |
| connect.timeout                            | optional | 30s     | Duration | 连接器在尝试连接到 MySQL 数据库服务器后超时前应等待的最长时间。 |
| connect.max-retries                        | optional | 3       | Integer  | 连接器应重试以建立 MySQL 数据库服务器连接的最大重试次数。    |
| connection.pool.size                       | optional | 20      | Integer  | 连接池大小。                                                 |
| jdbc.properties.*                          | optional | 20      | String   | 传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'. |
| heartbeat.interval                         | optional | 30s     | Duration | 用于跟踪最新可用 binlog 偏移的发送心跳事件的间隔。           |
| debezium.*                                 | optional | (none)  | String   | 将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 MySQL 服务器捕获数据更改。 For example: `'debezium.snapshot.mode' = 'never'`. 查看更多关于 [Debezium 的 MySQL 连接器属性](https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-connector-properties) |
| scan.incremental.close-idle-reader.enabled | optional | false   | Boolean  | 是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。 |

上述参数中，必填项的就不用太多解释了。选填项中消费数据的模式用的比较多。

FlinkCDC消费数据的模式常用的有如下两种：

* initial：初始化消费，消费历史数据+增量数据，它是默认的模式（全量+增量）。
* latest-offset：从最近的位置开始消费，消费增量数据，不会消费历史数据（仅增量）。

## 数据湖

### 生活中的湖

![1693316861884](assets/1693316861884.png)

* 生活中的湖是用来装什么？

装水。所以叫水湖。

* 水从哪里来？

地下的积水

天上的降水

高山上的流水

河流经过的积水

……

* 水有什么用？

在旱季来临的时候，用来灌溉农作物

* 水什么时候用？

需要的时候。**它是一种先存后用的思想。**

### 程序中的湖

* 程序中的湖是用来装什么的？

数据。大数据就是处理数据，所以叫数据湖。

* 数据从哪里来？

数据库、文件、日志、各式各样的数据都可以。

它可以存储结构化、半结构化、非结构化的数据。它可以存储原始数据。

* 数据有什么用？

现在是信息时代，数据是无价的。是公司中最昂贵的资产。

* 数据什么时候用呢？

需要的时候。**它是一种先存后用的思想。**

### 数据湖

