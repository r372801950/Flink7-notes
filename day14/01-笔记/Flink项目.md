# Flink项目

## 今日课程内容介绍

* Hudi查询数据的方式
    * 快照查询
    * 增量查询
    * 读优化查询
* Hudi的表类型
    * COW（Copy On Write）
    * MOR（Merge On Read）
* 综合案例
    * 湖仓一体的案例
* Doris概述
* Doris安装部署
* Doris原理
* Doris实践

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

什么是数据湖呢？数据湖必须满足如下2个点：

* 允许原始海量数据存储（结构化、半结构化、非结构化）
* 允许各种计算框架读取使用（Hive、Spark、Flink）

如下图所示：

![1693482406645](assets/1693482406645.png)

结论：数据湖是一种思想。它不涉及某一门具体的计算框架。

凡是实现了这种思想的框架，就是一个数据湖框架。

业界开源的数据湖框架有如下几个：

* Delta Lake
* Apache Iceberg
* Apache Hudi

### Delta Lake

架构如下：

![1693483047003](assets/1693483047003.png)

Delta Lake是Spark的商业公司（databricks，砖厂）推出来的数据湖框架，因此它和Spark计算框架强绑定。

### Apache Iceberg

如下图所示：

![1693483425881](assets/1693483425881.png)

Apache Iceberg，是Netflix（奈飞、网飞）公司推出了的数据湖框架，是一个抽象程度最高的数据湖，它不和任何计算框架绑定，天生是为对象存储而生的。

Iceberg和对象存储整合非常OK。

## Apache Hudi

### 概述

Hudi：Hadoop Upsert Delete and Incremental。从名字来看，可以看出，它能针对HDFS文件系统上的数据进行增删改查操作。

管理位于分布式文件系统上的数据集。

### 特点

![1693483935208](assets/1693483935208.png)

* Transaction（事务）
* 支持行级别的更新/删除操作
* 变更流

* 时间旅行（可以根据版本回退）
* 支持云平台

### 架构

![1693484390850](assets/1693484390850.png)

备注：这个架构是基于湖仓一体的。关于湖仓一体架构，稍后介绍。

### 历史

* 2015年提出来的论文
* 2016由Uber公司开发
* 2017年开源
* 2019年1月进入了Apache孵化
* 2020年5月孵化成功，成为顶级项目
* 截止2023年8月底，最新版为0.13.1
* 本次课程中使用的版本为0.11.1（2022年中旬的版本）

### 应用

![1693484825991](assets/1693484825991.png)

## 湖仓一体

湖仓一体：数据湖和数据仓库进行整合。

为什么要整合？

数据湖和数据仓库，单独拿出来，各有各的优缺点。而且各自非常互补。

### 数据仓库的特点

![1693485225800](assets/1693485225800.png)

优点：

* 支持结构化数据分析
* 安全性、私密性较好
* 更方便管理

缺点：

* 缺乏灵活性

* 不支持半结构化、非结构化数据分析
* 扩容代价比较昂贵

应用：

擅长BI，不擅长AI

### 数据湖

![1693485472648](assets/1693485472648.png)

优点：

* 支持各种数据的存储

* 扩容相比廉价
* 更新效率快

缺点：

* 快速沦为数据沼泽
* 数据管理不方便
* 安全性较弱

应用：

擅长于AI，不擅长BI

### 湖仓一体架构

把数据湖和数据仓库的优点结合起来，使用数据仓库的管理特性来管理数据湖的数据。

![1693485669328](assets/1693485669328.png)

湖仓一体是一种新的数据管理模式，将数据仓库和数据湖两者之间的差异进行融合，并将数据仓库构建在数据湖上，从而有效简化了企业数据的基础架构，提升数据存储弹性和质量的同时还能降低成本，减小数据冗余。

## Hudi入门案例

### 组件启动

~~~shell
#1.启动HDFS
start-dfs.sh

#2.启动Flink
start-cluster.sh

#3.进入FlinkSQL客户端
sql-client.sh
~~~

### Flink整合Hudi

从官网下载Flink整合Hudi的jar，并且把jar包放在`FLINK_HOME/lib`目录下，重启Flink集群即可。

项目中已经整合完成了。

### 操作

#### 添加数据

~~~shell
#1.创建表
CREATE TABLE t1(
  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://node1:8020/hudi/t1',
  'table.type' = 'MERGE_ON_READ'
);

#2.插入数据
INSERT INTO t1 VALUES
  ('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),
  ('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),
  ('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),
  ('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),
  ('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),
  ('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),
  ('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),
  ('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4');
~~~

#### 查询数据

~~~shell
select * from t1;
~~~

截图如下：

![1693488056654](assets/1693488056654.png)

#### 更新数据

~~~shell
insert into t1 values
  ('id1','Danny',27,TIMESTAMP '1970-01-01 00:00:01','par1');
~~~

截图如下：

![1693488230124](assets/1693488230124.png)

#### 流式读取

流式任务。不会结束。

~~~shell
#1.创建表
CREATE TABLE t2(
  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://node1:8020/hudi/t2',
  'table.type' = 'MERGE_ON_READ',
  'read.streaming.enabled' = 'true', 
  'read.start-commit' = '20210316134557', 
  'read.streaming.check-interval' = '4'
);

#2.插入数据
INSERT INTO t2 VALUES
  ('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),
  ('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),
  ('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),
  ('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),
  ('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),
  ('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),
  ('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),
  ('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4');
~~~

截图如下：

![1693488599234](assets/1693488599234.png)

#### 删除数据

目前删除不了，需要借助于关系型数据库来进行，后面再演示。

#### 结论

* Hudi的数据存储，底层是依赖于HDFS。Hudi本身不存储数据。
* Hudi支持行级别的更新、删除功能，扩展HDFS的能力

## Hudi核心概念剖析

![1693489069245](assets/1693489069245.png)

### Hudi数据管理

Hudi通过时间轴元数据、数据格式、索引来管理数据的。

现在分别来介绍。

#### 时间轴元数据

Hudi就是通过一个个的Instant（瞬态视图）来管理Hudi的表操作。

![1693489617479](assets/1693489617479.png)

#### 数据格式

Hudi的数据格式有两种：avro、parquet。这两种数据格式，有一定的组织结构。

![1693489764986](assets/1693489764986.png)

层级关系：

~~~shell
hudi表 -> 分区 -> FileGroup（文件组） -> FileSlice（文件片） -> （1个parquet+N个log）
~~~

log：avro文件，日志文件

parquet：base文件，压缩文件

#### 索引

Hudi支持索引，它的数据插入、更新，删除都需要依赖索引。效率也比较高。

Hudi的索引有如下几种：

* Bloom Index（布隆索引）默认的
* HBase Index（HBase索引）
* Simple Index（简单索引）
* Custom Index（自定义索引）

> A Bloom filter, named for its creator, Burton Howard Bloom, is a data structure which is designed to predict whether a given element is a member of a set of data. A positive result from a Bloom filter is not always accurate, but a negative result is guaranteed to be accurate.
>
> 布隆过滤器，它的本质就是一个数据结构。
>
> 它的目的就是用来判断数据是否在给定的数据集中。
>
> 如果返回结果是个正数，说明数据可能在集合中。
>
> 如果返回结果是个负数，说明数据一定不在集合中。
>
> 总结：布隆过滤器，可能会出现误判的情况。但是效率极高。

### Hudi的查询类型

Hudi的查询类型有三种：

* 快照查询（Snapshot Query）
* 增量查询（Incremental Query）
* 读优化查询（Read Optimized Query）

如下图所示：

![1693634095165](assets/1693634095165.png)

#### 快照查询

快照查询 = 查询（parquet文件+日志文件）

~~~shell
Snapshot Query = Query（parquet + log）
~~~

#### 增量查询

增量查询 = 查询（日志文件）

~~~shell
Incremental Query = Query（log）
~~~

#### 读优化查询

~~~shell
Read Optimized Query = Query（parquet）
~~~

### Hudi表类型

Hudi有两种类型的表：

* Copy On Write（写时复制，COW）

* Merge On Read（读时合并，MOR）

#### COW

COW，写时复制，也就是说，数据在写的时候，先把源表的数据进行拷贝（复制），然后在进行写入。

最终写完后，数据是最新的。

特点：

写的时候效率不高，但是读的时候，由于数据是最全的，不需要做额外的操作，因此效率很高。

使用场景：

适用于**写少读多**的场景。

如下图：

![1693634857067](assets/1693634857067.png)

案例：

![1693635237762](assets/1693635237762.png)

#### MOR

MOR，Merge On Read，读时合并。数据在写的时候，不影响，不会主动做任何操作。在读取数据的时候，会把写入的哪些数据文件进行合并，合并成一个base file（parquet），然后再读取。

特点：

数据写入时，由于没有数据拷贝的过程，因此非常高效。但是数据在读取时，由于要把数据进行合并，因此效率较低。

场景：

适合**写多读少**的场景。

如下图：

![1693637083359](assets/1693637083359.png)

案例：

![1693637654024](assets/1693637654024.png)

## Hudi综合案例

这个案例是一个湖仓一体的案例。

### 需求

~~~shell
通过FlinkCDC，把MySQL的数据同步到Hudi中，并且开启湖仓一体架构。
~~~

### 分析

![1693638934061](assets/1693638934061.png)

操作步骤：

~~~shell
在MySQL中准备库、表、数据（已经有了）
在FlinkSQL中创建MySQL源表的映射表（source_table）
在FlinkSQL中创建Hudi的映射表（sink_table）
拉起数据任务（insert into sink_table select xxxx from source_table）
校验数据（HDFS、Hive）
~~~

### 实现

#### 启动服务

~~~shell
#1.HDFS，校验路径是否存在
start-dfs.sh


#2.启动Flink
start-cluster.sh


#3.进入FlinkSQL客户端
sql-client.sh


#4.进入MySQL，校验bxg库下的oe_course_type表是否有数据
mysql -uroot -p123456


#5.Hive
nohup hive --service metastore > /tmp/hive-metastore.log &
~~~

#### 湖仓一体的配置

~~~shell
'hive_sync.enable'= 'true', -- 开启自动同步hive
'hive_sync.mode'= 'hms', -- 自动同步hive模式，默认jdbc模式
'hive_sync.metastore.uris'= 'thrift://192.168.88.161:9083', -- hive metastore地址
'hive_sync.table'= 'bxg_oe_course_type', -- hive 新建表名
'hive_sync.db'= 'bxg', -- hive 新建数据库名
'hive_sync.username'= '', -- HMS 用户名
'hive_sync.password'= '', -- HMS 密码
'hive_sync.support_timestamp'= 'true'-- 兼容hive timestamp类型
~~~

#### 实现

##### 在MySQL中准备库、表、数据

数据已经有了，略。

##### 在FlinkSQL中创建MySQL源表的映射表

列名一致、类型匹配上、表名随意。

~~~shell
CREATE TABLE if not exists mysql_bxg_oe_course_type (
      `id` INT,
      `type_code` STRING,
      `desc` STRING,
      `creator` STRING,
      `operator` STRING,
      `create_time` TIMESTAMP(3),
      `update_time` TIMESTAMP(3),
      `delete_flag` BOOLEAN,
      PRIMARY KEY (`id`) NOT ENFORCED
    ) WITH (
      'connector'= 'mysql-cdc',  -- 指定connector，这里填 mysql-cdc
      'hostname'= '192.168.88.161', -- MySql server 的主机名或者 IP 地址
      'port'= '3306',  -- MySQL 服务的端口号
      'username'= 'root',   --  连接 MySQL 数据库的用户名
      'password'='123456',  -- 连接 MySQL 数据库的密码
      'server-time-zone'= 'Asia/Shanghai',  -- 时区
      'debezium.snapshot.mode'='initial',  -- 启动模式，默认为initial
      'database-name'= 'bxg',  -- 需要监控的数据库名
      'table-name'= 'oe_course_type' -- 需要监控的表名
);
~~~

##### 在FlinkSQL中创建Hudi的映射表

~~~shell
CREATE TABLE if not exists hudi_bxg_oe_course_type (
         `id` INT,
         `type_code` STRING,
         `desc` STRING,
         `creator` STRING,
         `operator` STRING,
         `create_time` TIMESTAMP(3),
         `update_time` TIMESTAMP(3),
         `delete_flag` BOOLEAN,
     `partition` STRING,
     PRIMARY KEY (`id`) NOT ENFORCED
    ) PARTITIONED BY (`partition`)
    with(
       'connector'='hudi',
      'path'= 'hdfs://192.168.88.161:8020/hudi/bxg_oe_course_type',  -- 数据存储目录
      'hoodie.datasource.write.recordkey.field'= 'id', -- 主键
      'write.precombine.field'= 'update_time',  -- 自动precombine的字段
      'write.tasks'= '1',
      'compaction.tasks'= '1',
      'write.rate.limit'= '2000', -- 限速
      'table.type'= 'MERGE_ON_READ', -- 默认COPY_ON_WRITE,可选MERGE_ON_READ
      'compaction.async.enabled'= 'true', -- 是否开启异步压缩
      'compaction.trigger.strategy'= 'num_commits', -- 按次数压缩
      'compaction.delta_commits'= '1', -- 默认为5
      'changelog.enabled'= 'true', -- 开启changelog变更
      'read.tasks' = '1',
      'read.streaming.enabled'= 'true', -- 开启流读
      'read.streaming.check-interval'= '3', -- 检查间隔，默认60s
      'hive_sync.enable'= 'true', -- 开启自动同步hive
      'hive_sync.mode'= 'hms', -- 自动同步hive模式，默认jdbc模式
      'hive_sync.metastore.uris'= 'thrift://192.168.88.161:9083', -- hive metastore地址
      'hive_sync.table'= 'bxg_oe_course_type', -- hive 新建表名
      'hive_sync.db'= 'bxg', -- hive 新建数据库名
      'hive_sync.username'= '', -- HMS 用户名
      'hive_sync.password'= '', -- HMS 密码
      'hive_sync.support_timestamp'= 'true'-- 兼容hive timestamp类型
    );
~~~

##### 拉起数据任务

~~~shell
INSERT INTO hudi_bxg_oe_course_type SELECT  `id`,`type_code` ,`desc`,`creator` ,`operator`,`create_time` ,`update_time` ,`delete_flag`,DATE_FORMAT(`create_time`, 'yyyyMMdd') FROM mysql_bxg_oe_course_type;
~~~

##### 校验数据（HDFS、Hive）

* HDFS

![1693641750513](assets/1693641750513.png)

* Hive

![1693642794530](assets/1693642794530.png)

#### 表名后缀ro&rt解释

ro：Read Optimized，读优化的表。

rt：real-time，实时的表。这个表是最新的。

ro的表数据和rt的表数据，中间有时候会不一致，但是最终的表数据一定会一致。

而且他们两种类型的表的Schema是一致的。

## Doris

### 为什么学Doris

大数据计算分为两种：离线、实时。

离线：Hive（mr）、Spark

实时：Flink

场景：如果我们公司想做实时，但是又不懂Flink，怎么办？

Doris就可以满足这个需求。

Doris是一个实时分析型数据库。

### Doris概述

#### 概念

Doris，是一个基于MPP引擎的实时分析型数据库。

MPP：大规模并行处理。类似于MapReduce中的分而治之的思想。

#### 特点

* 支持大规模、海量数据的存储和分析
* 高性能、高可用、高并发
* 支持标准SQL、**兼容MySQL协议**
* 极简运维
* 生态丰富、并且支持多种导入数据的方式

#### 历史

* 2008年由百度研发，应用于百度的蜂巢报表场景

* 2012年发布了正式版1.0，开始支持分布式，承接百度所有的报表场景
* 2015年发布了正式版2.0，开始对外提供服务
* 2017年开源
* 2018年捐赠给了Apache，开始孵化
* 2022年6月孵化成功，成为顶级项目
* 目前最新版为2.0.0，项目中用的版本为1.1.0。

#### 架构

![1693645462356](assets/1693645462356.png)

Doris的位置就是一个实时分析的位置。

#### 使用

![1693645572249](assets/1693645572249.png)

国内大厂都在用。

#### OLAP、OLTP、HTAP

**OLAP**：Online Analysis Process，联机分析处理。对历史数据做分析。

离线：Hive、Spark

实时：Clickhouse、Druid、Kylin、TiDB、Doris

MOLAP：M：multi-dimension，多维分析。比如Druid、Kylin。

ROLAP：R：relationship，关系分析。保存着表与表之间的关系。比如Clickhouse、Doris。

Clickhouse和Doris：

Clickhouse，是俄罗斯的Yandex公司（搜索）研发的，单表性能非常强悍，多表join性能不佳，非标准SQL，运维很繁琐

Doris：是百度公司（搜索）研发的，单表性能比不上Clickhouse，但是多表join性能优于Clickhouse，标准SQL，极简运维

**OLTP**：Online Transaction Process，联机事务处理，一般值的是关系型数据库，比如：MySQL、Oracle、PostgreSQL等。

**HTAP**：Hybrid Transaction Analysis Process，混合事务分析处理，TiDB数据库，是一款集OLAP和OLTP于一身的数据库。既能做分析，有能做事务。

#### MySQL客户端 - 扩展

~~~shell
#语法
help + keyword

#案例
#1.查看+号的作用
help +;

#2.查看varchar类型
help varchar;

#3.除非终工具没有提示，则去其他地方参考，比如官网。
~~~

### Doris安装部署

在安装部署之前，先简单介绍下Doris的2组成：

* FE，Frontend，前端节点，用户查询请求，SQL解析，执行计划生成，元数据管理，节点管理等
* BE，Backend，后端节点，数据存储，查询计划执行

#### 配置FE

~~~shell
#1.修改priority_networks
priority_networks = 192.168.88.161/24

#2.配置meta_dir
meta_dir = /export/server/apache-doris-1.1.0-bin-x86-jdk8/fe/doris-meta
~~~

#### 启动FE

~~~shell
#1.进入FE的安装目录
cd /export/server/doris/fe

#2.启动FE
bin/start_fe.sh --daemon
~~~

#### 校验FE

~~~shell
#1.jps命令
PaloFe

#2.curl访问
curl http://127.0.0.1:8030/api/bootstrap

#3.WebUI
node1:8030

#4.后台校验
mysql -uroot -p123456 -hnode1 -P9030
show frontends\G;
~~~

#### 配置BE

~~~shell
#1.修改priority_networks
priority_networks = 192.168.88.161/24

#2.配置storage_root
storage_root_path = /export/server/apache-doris-1.1.0-bin-x86-jdk8/be/storage1,10;/export/server/apache-doris-1.1.0-bin-x86-jdk8/be/storage2
~~~

#### 启动BE

~~~shell
#1.进入FE的安装目录
cd /export/server/doris/be

#2.启动FE
bin/start_be.sh --daemon
~~~

#### 校验BE

~~~shell
#1.ps命令
ps -ef | grep be

#2.WebUI
8030端口登录后，通过System -> backends，就能查看后端节点的运行情况。

#3.后台校验
mysql -uroot -P9030 -hnode1 -p123456
show backends\G;
~~~

#### 启动Broker【可选】

broker的用处：用来和外部的文件系统打交道。

当需要和外部的文件系统打交道时，就需要Broker进程了。

Broker不需要配置。直接启动即可。

~~~shell
#1.切换Broker安装目录
cd /export/server/doris/apache_hdfs_broker

#2.启动Broker
bin/start_broker.sh --daemon
~~~

#### 校验Broker

~~~shell
#1.jps命令
BrokerBootstrap

#2.WebUI校验
8030端口登录后，通过System -> brokers，就能查看后端节点的运行情况。

#3.后台校验
mysql -uroot -P9030 -hnode1 -p123456
show broker\G;
~~~

#### 停止命令

~~~shell
#0.切换路径
cd /export/server/doris

#1.停止fe
fe/bin/stop_fe.sh

#2.停止be
be/bin/stop_be.sh

#3.停止broker
apache_hdfs_broker/bin/stop_broker.sh
~~~

#### 运维命令【了解】

~~~shell
#1.扩容
#1.1添加前端
ALTER SYSTEM ADD FRONTEND "192.168.0.1:9050";
#1.2添加后端
ALTER SYSTEM ADD BACKEND "192.168.0.1:9050";

#2.缩容
#1.1剔除前端
ALTER SYSTEM DROP FRONTEND "host1:port", "host2:port";
#1.2剔除后端
ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";
~~~

相比HDFS而言，Doris的运维就是一条命令。把节点扩容进去即可，不需要做数据均衡操作。

### Doris原理

#### 端口说明

Doris的端口非常多。

![1693657215279](assets/1693657215279.png)

我们只需要关注2个端口即可。

8030：FE的HTTP端口，我们可以通过这个端口，以web的方式访问FE

9030：BE的后台端口，我们可以通过这个端口，以MySQL客户端的形式来登录后端

#### 名词解释

- FE，Frontend，前端节点，用户查询请求，SQL解析，执行计划生成，元数据管理，节点管理等
- BE，Backend，后端节点，数据存储，查询计划执行

Doris的层次结构：Doris库 -> Doris表 -> Doris分区 -> Doris分桶

#### Doris架构

![1693657599406](assets/1693657599406.png)

前端节点和后端节点各自独立运行，互不影响。

架构：

Apache Doris = Google Mesa（数据模型） + Apache Impala（查询引擎） + ORC（列存）

数据模型：可以基于数据构建高效的数据模型，以供不同的查询需求时的使用

Impala：MPP的执行引擎

ORC：自研的列存

#### 数据分发

![1693657916307](assets/1693657916307.png)

Doris会自动进行副本的均衡，不需要用户手动参与，而且用户也没办法手动干预。

#### 元数据管理

元数据管理，通过Checkpoint + 内存 + JournalNode三个机制来保证高可用，高可靠，高效。

NameNode的高可用就是通过JournalNode来实现的。

### 实践

#### 建库

和MySQL一样。

~~~shell
#1.建库
create database test;

#2.切换库
use test;
~~~

#### 建表

~~~shell
#1.尝试使用MySQL的语法来建表
create table t1 (
id int,
name varchar(20),
sex int,
age int,
address varchar(50)
)
distributed by hash (id) buckets 1
properties(
"replication_num" = "1"
);


#2.上述建表会报错
ERROR 1105 (HY000): errCode = 2, detailMessage = Create olap table should contain distribution desc
缺少了分桶信息

#3.添加分桶后，还会报错，原因是副本数无法满足默认的3副本策略，因此需要修改副本数


#4.最终创建成功后，查看表结构，详细如下
CREATE TABLE `t1` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(20) NULL COMMENT "",
  `sex` int(11) NULL COMMENT "",
  `age` int(11) NULL COMMENT "",
  `address` varchar(50) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`, `name`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
);
~~~

建表语法：

~~~shell
CREATE TABLE [IF NOT EXISTS] [database.]table
(
    column_definition_list
    [, index_definition_list]
)
[engine_type]
[keys_type]
[table_comment]
[partition_info]
distribution_desc
[rollup_list]
[properties]
[extra_properties]
~~~

建表说明：

~~~shell
#1.字段的说明
Doris的字段，分为两种类型：
key类型：维度列，一般放在value列的前面，维度列是普通列
value类型：指标列，一般放在key列的后面，指标列是聚合列，指标列可以进行聚合计算，聚合函数有四类：
replace：替换，用新的value替换旧的value
sum：求和，把新的value和旧的value进行求和操作
max：求最大值，把新的value和旧的value进行求最大值操作
min：求最小值，把新的value和旧的value进行求最小值操作


#2.数据模型
Doris的数据模型有三种：
Aggregate Model：聚合模型，把相同的key下的value会被聚合操作（key全部相同），聚合函数有replace、sum、max、min四种。
Unique Model：唯一模型，会保证key列的唯一性，如果key列相同，则数据会被替换（replace）
Duplicate Model：冗余模型，允许原始数据的冗余存储，不会对数据做任何操作，想怎么存就怎么存
如果不指定，则默认就是Duplicate Model。
语法如下：
aggregate key('key1','key2'...)
unique key('key1','key2'...)
duplicate key('key1','key2'...)

#3.分区
Doris支持两种类型的分区：手动分区（自己管理的分区）和动态分区。动态分区后面详细介绍，今天先看手动分区。
手动分区可以分为两种类型：
range（范围）分区：一般用时间范围。
list（列表）分区：使用固定的值来分区。
分区可以省略，如果省略的话，默认Doris系统会创建一个分区，这个分区成为单分区，它的分区名字和表名一样。
这种很常用。
我们可以使用如下的命令来查看分区：
show partitions from + 表名;
show partitions from t1;
range分区语法如下：
partition by range('时间列') (
partition '分区名1' values less than ('分区值1'),
partition '分区名2' values less than ('分区值2')
)
list分区语法如下：
partition by list('时间列') (
partition '分区名1' values in ('分区值1'),
partition '分区名2' values in ('分区值2')
)


#4.分桶
Doris建表必须分桶，而且不能省略。目前1.1.0版本中，只支持hash分桶。
语法如下：
distributed by hash('分桶的字段') buckets N
N：表示桶 数量。


#5.引擎
Doris支持多种引擎，但是Doris不负责管理非OLAP的引擎数据。
默认的引擎就是OLAP。
语法：
engine = olap


#6.属性配置
Doris的属性配置和Hive的tblproperties配置一样，可以针对不同的Doris表进行个性化的配置。
语法：
properties('key' = 'value')
~~~

示例建表：

~~~shell
#1.讲义
CREATE TABLE test_table
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
    PARTITION p201706 VALUES LESS THAN ('2017-07-01'),
    PARTITION p201707 VALUES LESS THAN ('2017-08-01'),
    PARTITION p201708 VALUES LESS THAN ('2017-09-01')
)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");


#2.官网
CREATE TABLE IF NOT EXISTS test.example_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "user id",
    `date` DATE NOT NULL COMMENT "",
    `city` VARCHAR(20) COMMENT "",
    `age` SMALLINT COMMENT "",
    `sex` TINYINT COMMENT "",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT ""
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
~~~



