# Flink项目

## 今日内容介绍

* 数据流向案例

* 新媒体短视频课程报名分析看板案例

* 营收业绩整体情况看板案例

## 数据流向案例

### 需求

~~~shell
全流程跑通整个架构。
~~~

### 分析

![1694240434088](assets/1694240434088.png)

操作步骤：

~~~shell
#任务一
在MySQL中创建库、表，插入数据
在FlinkSQL中创建MySQL的映射表
在FlinkSQL中创建Hudi ODS层的映射表
拉起数据任务（insert into）
校验数据（HDFS、Hive）


#任务二
在FlinkSQL创建Hudi DWD层的映射表
拉起数据任务
校验数据（HDFS、Hive）


#任务三
在Doris中创建库、表
在FlinkSQL中创建Doris的映射表
拉起数据任务
校验数据（Doris）

#任务四
在FlinkSQL创建Hudi DWS层的映射表
拉起数据任务
校验数据（HDFS、Hive）

#任务五
在Doris中创建库、表
在FlinkSQL中创建Doris的映射表
拉起数据任务
校验数据（Doris）
~~~

### 服务启动

~~~shell
#1.启动HDFS
start-dfs.sh

#2.启动Hive
nohup hive --service metastore > /tmp/hive-metastore.log &
nohup hive --service hiveserver2 > /tmp/hive-hiveserver2.log &

#3.启动Flink
start-cluster.sh

#4.启动Doris
cd $DORIS_HOME
fe/bin/start_fe.sh --daemon
be/bin/start_be.sh --daemon

#5.进入FlinkSQL客户端
sql-client.sh
~~~

### 数据流转示意图

![1694242886583](assets/1694242886583.png)

### 实现

#### 任务一

##### 在MySQL中创建库、表，插入数据

~~~shell
#1.创建库
create database if not exists hudi_test;

#2.切换库
use hudi_test;

#3.创建表orders
CREATE TABLE `orders` (
    `id` int(11) NOT NULL,
    `pid` int(11) NOT NULL,
    `num` int(11) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#4.插入数据到orders
INSERT INTO `orders` VALUES (1,1,2),(2,1,13),(3,2,55);

#5.创建表product
CREATE TABLE `product` (
    `id` int(11) NOT NULL,
    `name` varchar(50) DEFAULT NULL,
    `price` decimal(10,4),
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#5.插入数据到product表
INSERT INTO `product` VALUES (1,'phone',5680),(2,'door',857),(3,'screen',3333);
~~~

##### 在FlinkSQL中创建MySQL的映射表

~~~shell
CREATE TABLE orders_mysql (
  id INT,
  pid INT,
  num INT,
  PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'node1',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'hudi_test',
    'table-name' = 'orders'
);



CREATE TABLE product_mysql (
   id INT,
   name STRING,
   price decimal(10,4),
   PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'node1',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'hudi_test',
    'table-name' = 'product'
);
~~~

##### 在FlinkSQL中创建Hudi ODS层的映射表

~~~shell
CREATE TABLE orders_hudi(
    id INT,
    pid INT,
    num INT,
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi-warehouse/hudi_test/orders'
    ,'hoodie.datasource.write.recordkey.field'= 'id'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true'
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest' 
    ,'read.streaming.check-interval'= '3' 
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083' 
    ,'hive_sync.table'= 'orders_hudi' 
    ,'hive_sync.db'= 'hudi_test' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);



CREATE TABLE product_hudi(
    id INT,
    name STRING,
    price decimal(10,4),
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi-warehouse/hudi_test/product'
    ,'hoodie.datasource.write.recordkey.field'= 'id'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000'
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true'
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' -- 开启流读
    ,'read.start-commit'='earliest' --如果想消费所有数据，设置值为earliest
    ,'read.streaming.check-interval'= '3' -- 检查间隔，默认60s
    ,'hive_sync.enable'= 'true' -- 开启自动同步hive
    ,'hive_sync.mode'= 'hms' -- 自动同步hive模式，默认jdbc模式
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083' -- hive metastore地址
    ,'hive_sync.table'= 'product_hudi' -- hive 新建表名
    ,'hive_sync.db'= 'hudi_test' -- hive 新建数据库名
    ,'hive_sync.username'= '' -- HMS 用户名
    ,'hive_sync.password'= '' -- HMS 密码
    ,'hive_sync.support_timestamp'= 'true'-- 兼容hive timestamp类型
);
~~~

##### 拉起数据任务（insert into）

~~~shell
insert into orders_hudi select id,pid,num from orders_mysql;


insert into product_hudi select id,name,price from product_mysql;
~~~

##### 校验数据（HDFS、Hive）

* HDFS校验

![1694244238268](assets/1694244238268.png)

![1694244293047](assets/1694244293047.png)

* Hive校验

![1694244257162](assets/1694244257162.png)

![1694244361800](assets/1694244361800.png)

#### 任务二

##### 在FlinkSQL创建Hudi DWD层的映射表

~~~shell
CREATE TABLE dwd_orders_product_hudi (
    id INT,
    name STRING,
    num INT,
    price decimal(10,4),
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi-warehouse/hudi_test/dwd_orders_product'
    ,'hoodie.datasource.write.recordkey.field'= 'id'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000'
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true',
    'read.tasks' = '1',
    'read.streaming.enabled'= 'true', -- 开启流读
    'read.start-commit'='earliest',--如果想消费所有数据，设置值为earliest
    'read.streaming.check-interval'= '3', -- 检查间隔，默认60s
    'hive_sync.enable'= 'true', -- 开启自动同步hive
    'hive_sync.mode'= 'hms', -- 自动同步hive模式，默认jdbc模式
    'hive_sync.metastore.uris'= 'thrift://node1:9083', -- hive metastore地址
    'hive_sync.table'= 'dwd_orders_product_hudi', -- hive 新建表名
    'hive_sync.db'= 'hudi_test', -- hive 新建数据库名
    'hive_sync.username'= '', -- HMS 用户名
    'hive_sync.password'= '', -- HMS 密码
    'hive_sync.support_timestamp'= 'true'-- 兼容hive timestamp类型
);
~~~

##### 拉起数据任务

~~~shell
insert into dwd_orders_product_hudi 
select
    orders_hudi.id as id,
    product_hudi.name as name,
    orders_hudi.num as num,
    product_hudi.price as price
from orders_hudi
inner join product_hudi on orders_hudi.pid = product_hudi.id;
~~~

##### 校验数据（HDFS、Hive）

* HDFS校验

![1694245898856](assets/1694245898856.png)

* Hive校验

![1694246003819](assets/1694246003819.png)

#### 任务三

##### 在Doris中创建库、表

~~~shell
#1.创建库
create database if not exists test;

#2.创建表
create table if not exists test.dwd_orders_product_doris
(
    id  int, 
    name string not null,
num INT,
price decimal(10,4)
) Unique Key (`id`)
comment ''
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

##### 在FlinkSQL中创建Doris的映射表

~~~shell
CREATE TABLE if not exists dwd_orders_product_doris (
    id INT,
    name STRING,
    num INT,
    price decimal(10,4),
    PRIMARY KEY(id) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'test.dwd_orders_product_doris'
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
insert into dwd_orders_product_doris
select
    id,
    name,
    num,
    price
from dwd_orders_product_hudi;
~~~

##### 校验数据（Doris）

![1694246370181](assets/1694246370181.png)

#### 任务四

##### 在FlinkSQL创建Hudi DWS层的映射表

~~~shell
CREATE TABLE dws_orders_product_hudi(
    name STRING,
    cnt BIGINT,
    price decimal(10,4),
    total_money decimal(10,4),
    PRIMARY KEY(name) NOT ENFORCED
) WITH (
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi-warehouse/hudi_test/dws_orders_product'
    ,'hoodie.datasource.write.recordkey.field'= 'id'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000'
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true',
    'read.tasks' = '1',
    'read.streaming.enabled'= 'true', -- 开启流读
    'read.start-commit'='earliest',--如果想消费所有数据，设置值为earliest
    'read.streaming.check-interval'= '3', -- 检查间隔，默认60s
    'hive_sync.enable'= 'true', -- 开启自动同步hive
    'hive_sync.mode'= 'hms', -- 自动同步hive模式，默认jdbc模式
    'hive_sync.metastore.uris'= 'thrift://node1:9083', -- hive metastore地址
    'hive_sync.table'= 'dws_orders_product_hudi', -- hive 新建表名
    'hive_sync.db'= 'hudi_test', -- hive 新建数据库名
    'hive_sync.username'= '', -- HMS 用户名
    'hive_sync.password'= '', -- HMS 密码
    'hive_sync.support_timestamp'= 'true'-- 兼容hive timestamp类型
);
~~~

##### 拉起数据任务

~~~shell
insert into dws_orders_product_hudi
select
    name,
    sum(num) as cnt,
    max(price) as price,
    sum(num)*max(price) as total_money
from dwd_orders_product_hudi
group by name;
~~~

##### 校验数据（HDFS、Hive）

* HDFS校验

![1694246708413](assets/1694246708413.png)

* Hive校验

![1694246812359](assets/1694246812359.png)

#### 任务五

##### 在Doris中创建库、表

~~~shell
#1.创建库
create database if not exists test;


#2.创建表
create table if not exists test.dws_orders_product_doris(
	name VARCHAR(32),
    cnt BIGINT,
    price decimal(10,4),
    total_money decimal(10,4)
) Unique Key (`name`)
DISTRIBUTED BY HASH(`name`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

##### 在FlinkSQL中创建Doris的映射表

~~~shell
CREATE TABLE if not exists dws_orders_product_doris (
    name string,
    cnt BIGINT,
    price decimal(10,4),
    total_money decimal(10,4),
    PRIMARY KEY(name) NOT ENFORCED
) WITH (
    'fenodes' = '192.168.88.161:8030'
    ,'table.identifier' = 'test.dws_orders_product_doris'
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
insert into dws_orders_product_doris
select
 name,
 cnt,
 price,
 total_money
from dws_orders_product_hudi;
~~~

##### 校验数据（Doris）

![1694247161705](assets/1694247161705.png)

### 数据变更

自己手动模拟业务数据变更，校验架构的时效性。

变更：增加、修改、删除。

#### 模拟新增操作

~~~shell
#1.新增订单
insert into orders values (4,3,10);
~~~

截图如下：

![1694247760763](assets/1694247760763.png)

#### 模拟修改操作

~~~shell
#修改订单数量
update orders set num = 100 where id = 4;
~~~

截图如下：

![1694247830019](assets/1694247830019.png)

#### 模拟删除操作

~~~shell
#删除订单
delete from orders where id = 1;
~~~

截图如下：

![1694247927041](assets/1694247927041.png)

## 新媒体短视频课程报名分析看板

### 需求

#### 指标

![1694249407921](assets/1694249407921.png)

共16个指标，可以分为两类：

* 单项课程营收分析（1-14个指标）
* 整体课程营收分析（15-16个指标）

#### 需求说明

##### 专项课程营收分析结果展示

![1694249499798](assets/1694249499798.png)

##### 整体营收分析结果展示

![1694249539423](assets/1694249539423.png)

#### 需求介绍

* 专项课程营收分析

![1694250667571](assets/1694250667571.png)

* 整体课程营收分析

![1694250648376](assets/1694250648376.png)

#### 业务表介绍

* oe_course表：课程表
* oe_stu_course表：学员课程表

* oe_order表：订单表

* oe_stu_course_order表：学员课程和订单的关联表（中间表，只有数据库表中是多对多才需要这么干）

### 分析

#### 建模

~~~shell
#1.分层
ods：源数据层，保存原始数据
dwd：中间宽表层，拉宽操作
dws：聚合层
~~~

### 实现

#### ODS层

##### 数据流图

![1694258374710](assets/1694258374710.png)

##### 操作步骤

~~~shell
在MySQL中准备库、表，插入数据（数据都有）
在FlinkSQL创建MySQL的映射表
在FlinkSQL创建Hudi ODS层的映射表
拉起数据任务（4个数据同步任务）
校验数据
~~~

##### 实现

###### 在MySQL中准备库、表，插入数据（数据都有）

数据已经有了，只需要校验数据即可。

~~~shell
#oe_course表，1084
select count(1) from oe_course;

#oe_stu_course，92332
select count(1) from oe_stu_course;

#oe_order，94197
select count(1) from oe_order;

#oe_stu_course_order，84157
select count(1) from oe_stu_course_order;
~~~

###### 在FlinkSQL创建MySQL的映射表

~~~shell
CREATE TABLE if not exists mysql_bxg_oe_stu_course_order (
    `id` INT,
    `student_course_id` INT,
    `order_id` STRING,
    `order_detail_id` STRING,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector'= 'mysql-cdc',
    'hostname'= 'node1',
    'port'= '3306',
    'username'= 'root',
    'password'='123456',
    'server-time-zone'= 'Asia/Shanghai',
    'debezium.snapshot.mode'='initial',
    'database-name'= 'bxg',
    'table-name'= 'oe_stu_course_order'
);





CREATE TABLE if not exists mysql_bxg_oe_stu_course (
    `id` INT,
    `student_id` STRING,
    `course_id` INT,
    `status` TINYINT,
    `contract_status` TINYINT,
    `learn_status` TINYINT,
    `service_days` SMALLINT,
    `service_expires` TIMESTAMP(3),
    `validity_days` INT,
    `validity_expires` TIMESTAMP(3),
    `terminate_cause` TINYINT,
    `effective_date` TIMESTAMP(3),
    `finished_time` TIMESTAMP(3),
    `total_progress` DECIMAL(10,2),
    `purchase_time` INT,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector'= 'mysql-cdc',
    'hostname'= 'node1',
    'port'= '3306',
    'username'= 'root',
    'password'='123456',
    'server-time-zone'= 'Asia/Shanghai',
    'debezium.snapshot.mode'='initial',
    'database-name'= 'bxg',
    'table-name'= 'oe_stu_course'
);




CREATE TABLE if not exists mysql_bxg_oe_order (
    `id` STRING,
    `channel` STRING,
    `student_id` STRING,
    `order_no` STRING,
    `total_amount` DECIMAL(10,2),
    `discount_amount` DECIMAL(10,2),
    `charge_against_amount` DECIMAL(10,2),
    `payable_amount` DECIMAL(10,2),
    `status` TINYINT,
    `pay_status` TINYINT,
    `pay_time` TIMESTAMP(3),
    `paid_amount` DECIMAL(10,2),
    `effective_date` TIMESTAMP(3),
    `terminal` TINYINT,
    `refund_status` TINYINT,
    `refund_amount` DECIMAL(10,2),
    `refund_time` TIMESTAMP(3),
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'node1',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'server-time-zone'= 'Asia/Shanghai',
    'debezium.snapshot.mode'='initial',
    'database-name'= 'bxg',
    'table-name' = 'oe_order'
);






CREATE TABLE if not exists mysql_bxg_oe_course (
    `id` INT,
    `grade_name` STRING,
    `bigimg_path` STRING,
    `video_url` STRING,
    `img_alt` STRING,
    `description` STRING,
    `detailimg_path` STRING,
    `smallimg_path` STRING,
    `sort` INT,
    `status` STRING,
    `learnd_count` INT,
    `learnd_count_flag` INT,
    `original_cost` DECIMAL(10,2),
    `current_price` DECIMAL(10,2),
    `course_length` DECIMAL(10,2),
    `menu_id` INT,
    `is_free` BOOLEAN,
    `course_detail` STRING,
    `course_detail_mobile` STRING,
    `course_detail1` STRING,
    `course_detail1_mobile` STRING,
    `course_plan_detail` STRING,
    `course_plan_detail_mobile` STRING,
    `course_detail2` STRING,
    `course_detail2_mobile` STRING,
    `course_outline` STRING,
    `common_problem` STRING,
    `common_problem_mobile` STRING,
    `lecturer_id` INT,
    `is_recommend` INT,
    `recommend_sort` INT,
    `qqno` STRING,
    `description_show` INT,
    `rec_img_path` STRING,
    `pv` INT,
    `course_type` INT,
    `default_student_count` INT,
    `study_status` INT,
    `online_course` INT,
    `course_level` INT,
    `content_type` INT,
    `recommend_type` INT,
    `employment_rate` STRING,
    `employment_salary` STRING,
    `score` STRING,
    `cover_url` STRING,
    `offline_course_url` STRING,
    `outline_url` STRING,
    `project_page_url` STRING,
    `preschool_test_flag` BOOLEAN,
    `service_period` INT,
    `included_validity_period` TINYINT,
    `validity_period` INT,
    `qualified_jobs` STRING,
    `work_year_min` INT,
    `work_year_max` INT,
    `promote_flag` BOOLEAN,
    `create_person` STRING,
    `update_person` STRING,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `is_delete` BOOLEAN,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector'= 'mysql-cdc',
    'hostname'= 'node1',
    'port'= '3306',
    'username'= 'root',
    'password'='123456',
    'server-time-zone'= 'Asia/Shanghai',
    'debezium.snapshot.mode'='initial',
    'database-name'= 'bxg',
    'table-name'= 'oe_course'
);
~~~

###### 在FlinkSQL创建Hudi ODS层的映射表

~~~shell
CREATE TABLE if not exists hudi_bxg_ods_oe_stu_course_order (
    `id` INT,
    `student_course_id` INT,
    `order_id` STRING,
    `order_detail_id` STRING,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
   PRIMARY KEY (id) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/ods_oe_stu_course_order'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'ods_oe_stu_course_order'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);




CREATE TABLE if not exists hudi_bxg_ods_oe_stu_course (
    `id` INT,
    `student_id` STRING,
    `course_id` INT,
    `status` INT,
    `contract_status` INT,
    `learn_status` INT,
    `service_days` INT,
    `service_expires` TIMESTAMP(3),
    `validity_days` INT,
    `validity_expires` TIMESTAMP(3),
    `terminate_cause` INT,
    `effective_date` TIMESTAMP(3),
    `finished_time` TIMESTAMP(3),
    `total_progress` DECIMAL(10,2),
    `purchase_time` INT,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
   PRIMARY KEY (id) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/ods_oe_stu_course'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'ods_oe_stu_course'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);



CREATE TABLE if not exists hudi_bxg_ods_oe_order (
    `id` STRING,
    `channel` STRING,
    `student_id` STRING,
    `order_no` STRING,
    `total_amount` DECIMAL(10,2),
    `discount_amount` DECIMAL(10,2),
    `charge_against_amount` DECIMAL(10,2),
    `payable_amount` DECIMAL(10,2),
    `status` INT,
    `pay_status` INT,
    `pay_time` TIMESTAMP(3),
    `paid_amount` DECIMAL(10,2),
    `effective_date` TIMESTAMP(3),
    `terminal` INT,
    `refund_status` INT,
    `refund_amount` DECIMAL(10,2),
    `refund_time` TIMESTAMP(3),
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
   PRIMARY KEY (id) NOT ENFORCED
)WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/ods_oe_order'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'ods_oe_order'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);




CREATE TABLE if not exists hudi_bxg_ods_oe_course(
    `id` INT,
    `grade_name` STRING,
    `bigimg_path` STRING,
    `video_url` STRING,
    `img_alt` STRING,
    `description` STRING,
    `detailimg_path` STRING,
    `smallimg_path` STRING,
    `sort` INT,
    `status` STRING,
    `learnd_count` INT,
    `learnd_count_flag` INT,
    `original_cost` DECIMAL(10,2),
    `current_price` DECIMAL(10,2),
    `course_length` DECIMAL(10,2),
    `menu_id` INT,
    `is_free` BOOLEAN,
    `course_detail` STRING,
    `course_detail_mobile` STRING,
    `course_detail1` STRING,
    `course_detail1_mobile` STRING,
    `course_plan_detail` STRING,
    `course_plan_detail_mobile` STRING,
    `course_detail2` STRING,
    `course_detail2_mobile` STRING,
    `course_outline` STRING,
    `common_problem` STRING,
    `common_problem_mobile` STRING,
    `lecturer_id` INT,
    `is_recommend` INT,
    `recommend_sort` INT,
    `qqno` STRING,
    `description_show` INT,
    `rec_img_path` STRING,
    `pv` INT,
    `course_type` INT,
    `default_student_count` INT,
    `study_status` INT,
    `online_course` INT,
    `course_level` INT,
    `content_type` INT,
    `recommend_type` INT,
    `employment_rate` STRING,
    `employment_salary` STRING,
    `score` STRING,
    `cover_url` STRING,
    `offline_course_url` STRING,
    `outline_url` STRING,
    `project_page_url` STRING,
    `preschool_test_flag` BOOLEAN,
    `service_period` INT,
    `included_validity_period` INT,
    `validity_period` INT,
    `qualified_jobs` STRING,
    `work_year_min` INT,
    `work_year_max` INT,
    `promote_flag` BOOLEAN,
    `create_person` STRING,
    `update_person` STRING,
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `is_delete` BOOLEAN,
   PRIMARY KEY (id) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/ods_oe_course'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
,'read.streaming.enabled'= 'true'
    ,'read.start-commit'='earliest' 
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'ods_oe_course'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);
~~~

###### 拉起数据任务（4个数据同步任务）

~~~shell
INSERT INTO `hudi_bxg_ods_oe_stu_course_order` SELECT `id`, `student_course_id`, `order_id`, `order_detail_id`, `create_time`, `update_time`, `delete_flag`
FROM `mysql_bxg_oe_stu_course_order`;



INSERT INTO `hudi_bxg_ods_oe_stu_course` SELECT  id, student_id, course_id, status, contract_status, learn_status, service_days, service_expires, validity_days, validity_expires, terminate_cause, effective_date, finished_time, total_progress, purchase_time, create_time, update_time, delete_flag
FROM `mysql_bxg_oe_stu_course`;




INSERT INTO `hudi_bxg_ods_oe_order` SELECT  `id`, `channel`, `student_id`, `order_no`, `total_amount`, `discount_amount`, `charge_against_amount`, `payable_amount`, `status`, `pay_status`, `pay_time`, `paid_amount`, `effective_date`, `terminal`, `refund_status`, `refund_amount`, `refund_time`, `create_time`, `update_time`, `delete_flag`
FROM `mysql_bxg_oe_order`;




INSERT INTO `hudi_bxg_ods_oe_course`
select  id, grade_name, bigimg_path, video_url, img_alt, description, detailimg_path, smallimg_path, sort, status, learnd_count, learnd_count_flag, original_cost, current_price, course_length, menu_id, is_free, course_detail, course_detail_mobile, course_detail1, course_detail1_mobile, course_plan_detail, course_plan_detail_mobile, course_detail2, course_detail2_mobile, course_outline, common_problem, common_problem_mobile, lecturer_id, is_recommend, recommend_sort, qqno, description_show, rec_img_path, pv, course_type, default_student_count, study_status, online_course, course_level, content_type, recommend_type, employment_rate, employment_salary, score, cover_url, offline_course_url, outline_url, project_page_url, preschool_test_flag, service_period, included_validity_period, validity_period, qualified_jobs, work_year_min, work_year_max, promote_flag, create_person, update_person, create_time, update_time, is_delete
from `mysql_bxg_oe_course`;
~~~

###### 校验数据

* 8081页面

![1694259492548](assets/1694259492548.png)

* HDFS校验

![1694259530601](assets/1694259530601.png)

* Hive校验

![1694259414488](assets/1694259414488.png)

![1694259610655](assets/1694259610655.png)

![1694259661232](assets/1694259661232.png)

![1694259705278](assets/1694259705278.png)

#### Hudi DWD层

##### 宽表构建

需要求16个指标，16个可以两类：

（1）专项课程营收

（2）整体营收分析

共同点：

无论是那种类型，核心都是这3个指标：订单量、总金额、均价。

这3个指标，核心就是2个。订单量和总金额。

这2个指标和订单表相关（oe_order）。

不同点：

专项课程营收分析，是3张表的join（inner）。也就是课程表（oe_course）。

整体营收分析，是4张表的join（left）主表就是学生课程订单关联表（中间表）。

实现思路有：

**思路一**：构建2个宽表。一张宽表用于专项课程营收分析。另外一张宽表用于整体营收分析。

这两张宽表各自完成各自的指标需求，这种方式简单，直接，易理解，通用，是推荐的方式。

**思路二**：

因为指标都是一样的，其次表是相同的。能不能构建一张宽表，这张宽表包含全部需要的数据字段。

在这个需求里，是可以的。只是实现起来，难度大一点。

join如何选择？

前3张表是inner join，后四张表是left join，最终的宽表，如何选择join方式。

最终选择使用left join。保留最全的数据，同时可以使用条件，由left join的结果得到inner join的结果。

在这个实现中，我们采用方式二来实现。

字段如何选择？

字段的选择，首先应该是把必用的字段拿过来，其次可以根据经验，添加认为有用的字段。

~~~shell
ods_oe_stu_course_order（osco）： id, student_course_id, order_id
ods_oe_stu_course（osc）：course_id、status、delete_flag 
ods_oe_order（oo）：payable_amount、pay_status、pay_time、paid_amount、refund_status、delete_flag
ods_oe_course（oe）：grade_name
~~~

> 说明：
>
> 这些字段中，还可以添加表中人为有用的字段，都可以。

其次，还可以把一些公共的条件转换为字段。

比如SQL中大量频繁出现的判断语句：

~~~shell
WHERE oo.payable_amount > 0
  AND oo.pay_status = 2
  AND oo.delete_flag = 0
  AND osc.delete_flag = 0
~~~

组装宽表：

![1694262527666](assets/1694262527666.png)

上述字段有相同的，为了避免误解，改个名字：

![1694262736973](assets/1694262736973.png)

这就是最终的宽表字段了。

##### 数据流图

![1694263093260](assets/1694263093260.png)

操作步骤：

~~~shell
在FlinkSQL创建Hudi DWD层的映射表
拉起数据任务
校验数据（HDFS、Hive）
~~~

##### 实现

###### 在FlinkSQL创建Hudi DWD层的映射表

~~~shell
CREATE TABLE if not exists hudi_dwd_oe_stu_course_order (
     `id` int,
     `stu_course_id` int,
     `order_id` string,
     `course_id` int,
     `stu_course_status` int,
     `stu_course_status_des` string,
     `stu_course_delete_flag` BOOLEAN,
     `payable_amount` decimal(10,2),
     `pay_status` int,
     `pay_time` TIMESTAMP(3),
     `paid_amount` decimal(10,2),
     `refund_status` int,
     `order_delete_flag` boolean,
     `grade_name` string,
     `is_complete_order` boolean,
     PRIMARY KEY (`id`) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/dwd_oe_stu_course_order'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '3'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'dwd_oe_stu_course_order'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);
~~~

###### 拉起数据任务

~~~shell
insert into hudi_dwd_oe_stu_course_order
SELECT
    `osco`.`id`,
    `osco`.`student_course_id`,
    `osco`.`order_id`,
    `osc`.`course_id`,
    `osc`.`status` as `stu_course_status`,
     case `osc`.`status` when 0 then '试学' when 1 then '生效' when 2 then '待生效' when -1 then '停课' else '退费' end as `stu_course_status_des`,
    `osc`.`delete_flag` as `stu_course_delete_flag`,
    `oo`.`payable_amount`,
    `oo`.`pay_status`,
    `oo`.`pay_time`,
    `oo`.`paid_amount`,
    `oo`.`refund_status`,
    `oo`.`delete_flag` as `order_delete_flag`,
    `oc`.`grade_name`,
    if (oo.`payable_amount`>0 and `oo`.`pay_status`=2 and `oo`.`delete_flag` = false and `osc`.`delete_flag` = false, true, false) as is_complete_order
FROM hudi_bxg_ods_oe_stu_course_order AS osco
LEFT JOIN hudi_bxg_ods_oe_stu_course AS osc
ON osc.id = osco.student_course_id
LEFT JOIN hudi_bxg_ods_oe_order AS oo
ON oo.id = osco.order_id
LEFT JOIN hudi_bxg_ods_oe_course AS oc
ON oc.id = osc.course_id;
~~~

###### 校验数据（HDFS、Hive）

* HDFS校验

![1694263417220](assets/1694263417220.png)

* Hive校验

![1694263595996](assets/1694263595996.png)

#### Doris DWD层

##### 数据流图

![1694264995894](assets/1694264995894.png)

##### 操作步骤

~~~shell
在Doris中创建库、表
在FlinkSQL创建Doris的映射表
拉起数据任务
校验数据（Doris）
~~~

##### 实现

###### 在Doris中创建库、表

~~~shell
#1.创建库
CREATE DATABASE IF NOT EXISTS bxg;

#2.切换库
use bxg;

#3.创建表
CREATE TABLE IF NOT EXISTS bxg.dwd_oe_stu_course_order
(
   `id` int,
   `stu_course_id` int COMMENT '学员课程id',
   `order_id` string,
   `course_id` int COMMENT '学员购买的课程',
   `stu_course_status` int COMMENT '学员课程状态：0试学、1生效、2待生效、-1停课、8退费',
   `stu_course_status_des` string COMMENT '学员课程状态描述：0试学、1生效、2待生效、-1停课、8退费',
   `stu_course_delete_flag` BOOLEAN,
   `payable_amount` decimal(10,2) COMMENT '实际应付总金额=原价-优惠总额-冲抵金额',
   `pay_status` int  COMMENT '支付状态：0未支付、1部分支付、2支付完成',
   `pay_time` datetime COMMENT '最后支付完成时间',
   `paid_amount` decimal(10,2) COMMENT '当前已付总额',
   `refund_status` INT COMMENT '退费状态:0-未退费;-1-已退费;-2-退费中;-3-部分退费',
   `order_delete_flag` BOOLEAN COMMENT 'ods_bxg_oe_order表中订单是否删除',
   `grade_name` string COMMENT '课程名称',
   `is_complete_order` BOOLEAN COMMENT '实际应付总金额0且支付状态pay_status完成'
) Unique Key (`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

###### 在FlinkSQL创建Doris的映射表

~~~shell
CREATE TABLE if not exists doris_dwd_oe_stu_course_order (
     `id` int,
     `stu_course_id` int,
     `order_id` string,
     `course_id` int,
     `stu_course_status` int,
     `stu_course_status_des` string,
     `stu_course_delete_flag` BOOLEAN,
     `payable_amount` decimal(10,2),
     `pay_status` int,
     `pay_time` TIMESTAMP(3),
     `paid_amount` decimal(10,2),
     `refund_status` int,
     `order_delete_flag` boolean,
     `grade_name` string,
     `is_complete_order` boolean,
     PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'bxg.dwd_oe_stu_course_order'
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

###### 拉起数据任务

~~~shell
INSERT INTO doris_dwd_oe_stu_course_order SELECT `id`,`stu_course_id`, `order_id`,`course_id`,`stu_course_status`,`stu_course_status_des`, `stu_course_delete_flag`,`payable_amount`,`pay_status`,`pay_time`,`paid_amount`,`refund_status`, `order_delete_flag`, `grade_name`, `is_complete_order`
FROM hudi_dwd_oe_stu_course_order;
~~~

###### 校验数据（Doris）

![1694265305479](assets/1694265305479.png)

Doris的数据来自于Hudi的DWD层。因此，这个表的Schema和数据量一定要和Hudi的DWD层相同才可以。否则，说明操作有问题。

#### Hudi DWS层

##### 表的构建

这一层，本质上就是对宽表数据进行聚合，统计聚合结果。

所以，这一层中，我们可以根据宽表来统计指标的结果。

表如何构建？

由于需求中给了最终的展示结果，这里的聚合表可以参考展示的结构来进行构建。

通过需求中的结果展示，可以分析出，我们需要构建2张DWS层的聚合表。

（1）用来构建1-14个指标的聚合

（2）用来构建15-16个指标的聚合

##### 数据流图

![1694266112130](assets/1694266112130.png)

操作步骤：

~~~shell
创建Hudi DWS层的映射表
拉起数据任务
校验数据
~~~

##### 实现

###### 创建Hudi DWS层的映射表

~~~shell
CREATE TABLE if not exists hudi_dws_course_revenue(
    `course_id` int,
    `date` string,
    `total_cnt` bigint,
    `toatal_money` decimal(38,4),
    `avg` decimal(38,4),
    `stu_course_order_status` string,
    PRIMARY KEY (`course_id`,`date`) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/dws_course_revenue'
    ,'hoodie.datasource.write.recordkey.field'= '`course_id`,`date`'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000'
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true'
    ,'read.tasks' = '3'
    ,'read.streaming.enabled'= 'true'
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true'
    ,'hive_sync.mode'= 'hms'
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'dws_course_revenue'
    ,'hive_sync.db'= 'bxg'
    ,'hive_sync.username'= ''
    ,'hive_sync.password'= ''
    ,'hive_sync.support_timestamp'= 'true'
);



CREATE TABLE if not exists hudi_dws_overall_revenue (
    `course_id` int,
    `course_name` string,
    `paid_count` bigint,
    `paid_amount` decimal(38,4),
    PRIMARY KEY (`course_id`) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/dws_overall_revenue'
    ,'hoodie.datasource.write.recordkey.field'= '`course_id`'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000'
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true'
    ,'read.tasks' = '3'
    ,'read.streaming.enabled'= 'true'
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true'
    ,'hive_sync.mode'= 'hms'
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'dws_overall_revenue'
    ,'hive_sync.db'= 'bxg'
    ,'hive_sync.username'= ''
    ,'hive_sync.password'= ''
    ,'hive_sync.support_timestamp'= 'true'
);
~~~

###### 拉起数据任务

~~~shell
INSERT INTO hudi_dws_course_revenue
SELECT
    ifnull(course_id,-1) as course_id,
    '总计' AS `date`,
    count(1) AS `total_cnt`,
    CASE WHEN count(1) >0 THEN sum(paid_amount) ELSE 0 END  AS  `toatal_money`,
    CASE WHEN count(1) >0 THEN sum(paid_amount) / if(count(1)<=0,1,count(1)) ELSE 0 END AS `avg`,
    CONCAT('【',cast(course_id as string),'】',grade_name)  as  `stu_course_order_status`
FROM hudi_dwd_oe_stu_course_order
WHERE is_complete_order = true
  AND stu_course_status not in (8)
GROUP BY course_id,grade_name

union

select
    ifnull(course_id,-1) as course_id,
    ifnull(date_format(pay_time, 'yyyy/MM/dd'),'-1') as `date`,
    count(1) AS `total_cnt`,
    CASE WHEN count(1) > 0 THEN sum(paid_amount) ELSE 0 END AS `toatal_money`,
    CASE WHEN count(1) > 0 THEN sum(paid_amount) / if(count(1)=0,1,count(1)) ELSE 0 END AS `avg`,
    LISTAGG(stu_course_status_des) as `stu_course_order_status`
from hudi_dwd_oe_stu_course_order
WHERE is_complete_order is true
group by course_id,date_format(pay_time, 'yyyy/MM/dd');






INSERT INTO hudi_dws_overall_revenue
SELECT
    ifnull(course_id,-1) as course_id,
    grade_name AS `course_name`,
    COUNT(CASE WHEN (is_complete_order is true AND refund_status not in (-1))
                   THEN order_id
               ELSE null
        END)  AS `paid_count`,
    SUM(CASE WHEN (is_complete_order is true AND refund_status not in (-1))
                 THEN paid_amount
             ELSE null
        END) AS  `paid_amount`
FROM hudi_dwd_oe_stu_course_order
GROUP BY course_id,grade_name;
~~~

###### 校验数据

* HDFS校验

![1694267128472](assets/1694267128472.png)

* Hive校验

![1694267226713](assets/1694267226713.png)

![1694267293600](assets/1694267293600.png)

#### Doris DWS层

##### 数据流图

![1694267699632](assets/1694267699632.png)

操作步骤：

~~~shell
在Doris中创建库、表
在FlinkSQL创建Doris的映射表
拉起数据任务
校验数据
~~~

##### 实现

###### 在Doris中创建库、表

~~~shell
CREATE TABLE IF NOT EXISTS bxg.dws_course_revenue
(
    `course_id` int,
    `date` varchar(255),
    `total_cnt` bigint,
    `toatal_money` decimal(27,4),
    `avg` decimal(27,4),
    `stu_course_order_status` string
) Unique Key (`course_id`,`date`)
DISTRIBUTED BY HASH(`course_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);



CREATE TABLE IF NOT EXISTS bxg.dws_overall_revenue
(
    `course_id` int,
    `course_name` string,
    `paid_count` bigint,
    `paid_amount` decimal(27,4)
) Unique Key (`course_id`)
DISTRIBUTED BY HASH(`course_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

###### 在FlinkSQL创建Doris的映射表

~~~shell
CREATE TABLE if not exists doris_dws_course_revenue(
    `course_id` int,
    `date` string,
    `total_cnt` bigint,
    `toatal_money` decimal(38,4),
    `avg` decimal(38,4),
    `stu_course_order_status` string,
    PRIMARY KEY (`course_id`,`date`) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'bxg.dws_course_revenue'
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



CREATE TABLE if not exists doris_dws_overall_revenue(
    `course_id` int,
    `course_name` string,
    `paid_count` bigint,
    `paid_amount` decimal(38,4),
    PRIMARY KEY (`course_id`) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'bxg.dws_overall_revenue'
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

###### 拉起数据任务

~~~shell
insert into doris_dws_course_revenue
select `course_id`, `date`, `total_cnt`, `toatal_money`, `avg`, `stu_course_order_status`
from hudi_dws_course_revenue;



insert into doris_dws_overall_revenue
select `course_id`, `course_name`,`paid_count`,`paid_amount`
from hudi_dws_overall_revenue;
~~~

###### 校验数据

![1694267994981](assets/1694267994981.png)

### 业务查询

业务查询SQL，不用我没写，是业务写。

~~~shell
#1-14个指标
SELECT 
	`date` as `日期`,
	total_cnt as `全款量`,
	toatal_money as `全款额`,
	`avg` as `成交均价`,
	stu_course_order_status as `课程状态`
	from bxg.dws_course_revenue
	where course_id = 958
ORDER BY `日期` DESC;


SELECT 
	`date` as `日期`,
	total_cnt as `全款量`,
	toatal_money as `全款额`,
	`avg` as `成交均价`,
	stu_course_order_status as `课程状态`
	from bxg.dws_course_revenue
	where course_id = 1121
ORDER BY `日期` DESC;


#15-16个指标
SELECT
    course_id AS `课程id`,
    course_name AS `课程名称`,
    paid_count AS `全款量`,
    paid_amount AS `全款额`,
    paid_amount/paid_count AS `成交均价`
FROM bxg.dws_overall_revenue
where course_id in (958,1121,1129)
ORDER BY `课程id` DESC;
~~~

截图如下：

![1694268275718](assets/1694268275718.png)

## 营收业绩整体情况看板

### 需求

#### 指标

![1694518915593](assets/1694518915593.png)

#### 需求说明

![1694519190857](assets/1694519190857.png)

#### 指标解释

![1694519398312](assets/1694519398312.png)

进班：进班上课。

全款：交齐学费。不一定开始上课。

#### 结果展示

![1694519542777](assets/1694519542777.png)

#### SQL参考

~~~shell
SELECT
        sum(oo.`payable_amount` + (CASE WHEN oo.`charge_against_amount` IS NOT null THEN oo.`charge_against_amount` ELSE 0 END))/ 10000
FROM
    bxg.`oe_order` oo
        LEFT JOIN bxg.oe_stu_course_order oso ON oo.`id` = oso.`order_id`
            LEFT JOIN bxg.oe_stu_course osc ON osc.`id` = oso.`student_course_id`
            LEFT JOIN bxg.oe_course  oc ON osc.`course_id` = oc.`id`
WHERE
-- 支付状态：支付完成
        oo.`pay_status` = 2
-- 未删除订单
  AND oo.`delete_flag` = 0
-- 转班情况只取第一次的订单，转班后的订单不重复计算
  AND oo.`id` NOT IN (SELECT target_order_id FROM
    bxg.oe_order_transfer_apply t
                      WHERE t.biz_type = 1 AND t.status = 0
                        AND t.fee_transfer_type=0 AND t.delete_flag = 0)
-- 排除N12分摊转移
  AND oo.`terminal` != 7
-- 排除测试课
  AND oc.`id` NOT IN (555,1537)
-- 取当前年份
  AND year(oo.`pay_time`) = year(current_date());
~~~

要去除转班的数据。这个数据会让订单量和金额重复计算。

### 分析

#### 表说明

这个看板一共涉及到5张表：前面看板的4张表+转班申请表。

~~~shell
1）bxg.oe_order（订单：主订单）
2）bxg.oe_stu_course_order（学员课程和订单的关联，注意：将来可能会有一个学员课程对应多个订单的情况）
3）bxg.oe_stu_course（学员课程，只有 试学&学员支付成功 以后才会有该记录。将来可能会有一个课程对应多个订单的情况，所以这里不与订单直接关联！）
4）bxg.oe_course（课程，含就业课和微课）
5）bxg.oe_order_transfer_apply（转线下、线上互转申请表）
~~~

多了一张转班申请表。

#### 分层说明

分为3层。

ODS：源数据层

DWD：宽表层

DWS：聚合层

### 实现

#### ODS层

##### 数据流图

![1694521215272](assets/1694521215272.png)

操作步骤：

~~~shell
在FlinkSQL中创建MySQL的映射表
在FlinkSQL中创建Hudi ODS层的映射表
拉起数据任务
校验数据（HDFS、Hive）
~~~

##### 实现

###### 在FlinkSQL中创建MySQL的映射表

~~~shell
CREATE TABLE if not exists mysql_bxg_oe_order_transfer_apply (
  `id` INT,
  `order_id` STRING,
  `order_detail_id` STRING,
  `deposit_id` STRING,
  `cash_back_record_id` INT,
  `student_id` STRING,
  `course_id` INT,
  `stu_course_id` INT,
  `order_refund_id` INT,
  `original_stu_course_status` TINYINT,
  `original_order_refund_status` TINYINT,
  `biz_type` TINYINT,
  `oa_affair_id` STRING,
  `oa_summary_id` STRING,
  `oa_template_code` STRING,
  `oa_template_id` STRING,
  `oa_bill_no` STRING,
  `fee_transfer_type` TINYINT,
  `amount` DECIMAL(10,2),
  `status` TINYINT,
  `order_type` TINYINT,
  `target_order_id` STRING,
  `target_order_detail_id` STRING,
  `target_import_order_id` INT,
  `target_order_type` TINYINT,
  `creator` STRING,
  `creator_name` STRING,
  `create_time` TIMESTAMP(3),
  `update_time` TIMESTAMP(3),
  `delete_flag` BOOLEAN,
PRIMARY KEY (`id`) NOT ENFORCED
 ) WITH (
          'connector'= 'mysql-cdc',
          'hostname'= 'node1',
          'port'= '3306',
          'username'= 'root',
          'password'='123456',
          'server-time-zone'= 'Asia/Shanghai',
          'debezium.snapshot.mode'='initial',
          'database-name'= 'bxg',
          'table-name'= 'oe_order_transfer_apply'
          );
~~~

###### 在FlinkSQL中创建Hudi ODS层的映射表

~~~shell

CREATE TABLE IF NOT EXISTS `hudi_bxg_ods_oe_order_transfer_apply` (
   `id` INT,
  `order_id` STRING,
  `order_detail_id` STRING,
  `deposit_id` STRING,
  `cash_back_record_id` INT,
  `student_id` STRING,
  `course_id` INT,
  `stu_course_id` INT,
  `order_refund_id` INT,
  `original_stu_course_status` INT,
  `original_order_refund_status` INT,
  `biz_type` INT,
  `oa_affair_id` STRING,
  `oa_summary_id` STRING,
  `oa_template_code` STRING,
  `oa_template_id` STRING,
  `oa_bill_no` STRING,
  `fee_transfer_type` INT,
  `amount` DECIMAL(10,2),
  `status` INT,
  `order_type` INT,
  `target_order_id` STRING,
  `target_order_detail_id` STRING,
  `target_import_order_id` INT,
  `target_order_type` INT,
  `creator` STRING,
  `creator_name` STRING,
  `create_time` TIMESTAMP(3),
  `update_time` TIMESTAMP(3),
  `delete_flag` BOOLEAN, 
  PRIMARY KEY ( `id` ) NOT ENFORCED
) WITH(
     'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/ods_oe_order_transfer_apply'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'ods_oe_order_transfer_apply'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);
~~~

###### 拉起数据任务

~~~shell

INSERT INTO `hudi_bxg_ods_oe_order_transfer_apply` 
SELECT `id`,`order_id` ,`order_detail_id`,`deposit_id`,`cash_back_record_id` ,`student_id` ,`course_id`,`stu_course_id` ,`order_refund_id` ,`original_stu_course_status` ,`original_order_refund_status` ,`biz_type`,`oa_affair_id`,`oa_summary_id` ,`oa_template_code` ,`oa_template_id`,`oa_bill_no`,`fee_transfer_type` ,`amount`,`status`,`order_type` ,`target_order_id`,`target_order_detail_id` ,`target_import_order_id`,`target_order_type` ,`creator` ,`creator_name`,`create_time`,`update_time`,`delete_flag`
FROM `mysql_bxg_oe_order_transfer_apply`;
~~~

###### 校验数据（HDFS、Hive）

* HDFS校验

![1694522056806](assets/1694522056806.png)

* Hive校验

![1694522157055](assets/1694522157055.png)

#### Hudi DWD层

##### 宽表构建

DWD层可以根据最终的指标来构建。

方式一：基于指标来构建

因为指标主要是营收额和订单量，且数据源表只有5张表。

这5张表，有4张都是和第一个看板相同的表，只有一张新表。

直接拿着5张表来构建。把这5张表构建成一张宽表。

这种方式很通用，也容易理解。也是推荐的方案。

方式二：基于之前的宽表构建

因为这5张表，有4张都是和第一个看板相同的表，只有一张新表。

因此，我可以把原来的宽表进行删除，仍然使用这4张表重新构建一张更大的宽表。

这张宽表不仅可以应用之前的看板，也可以给现在的看板。

还剩下一张表（转班申请表），这张转班申请表，可以和订单表一起，额外构建成一张小宽表。

因此，整体需要构建2张宽表：

宽表一：原来的4张表构建（字段更多）

宽表二：订单表和转班申请表额外构建一张小宽表。

这种方式优点：节省存储空间。

删除原宽表：

~~~shell
#1.删除Hudi的宽表数据（HDFS）
删除HDFS的/hudi/bxg/dwd_oe_stu_course_order路径

#2.删除Hive的宽表数据（Hive）
drop table dwd_oe_stu_course_order_ro;
drop table dwd_oe_stu_course_order_rt;

#3.删除Doris宽表数据（Doris）
drop table dwd_oe_stu_course_order;
~~~

##### 数据流图

![1694524391796](assets/1694524391796.png)

操作步骤：

~~~shell
在FlinkSQL创建Hudi DWD层的映射表（2张表）
拉起数据任务（2个任务）
校验数据（HDFS、Hive）
~~~

##### 实现

###### 在FlinkSQL创建Hudi DWD层的映射表（2张表）

~~~shell
CREATE TABLE if not exists hudi_dwd_oe_stu_course_order (
     `id` int,
     `stu_course_id` int,
     `order_id` string,
     `course_id` int,
     `stu_course_status` int,
`stu_course_status_des` string,
     `stu_course_delete_flag` BOOLEAN,
`effective_date` TIMESTAMP(3),
`payable_amount` decimal(10,2),
`pay_status` int,
`pay_time` TIMESTAMP(3),
`paid_amount` decimal(10,2),
`refund_status` int,
`order_delete_flag` boolean,
`grade_name` string,
`course_type` INT,
`is_complete_order` boolean,
PRIMARY KEY (`id`) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/dwd_oe_stu_course_order'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'dwd_oe_stu_course_order'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);






CREATE VIEW IF NOT EXISTS bxg_common_change_classes_v AS SELECT distinct(target_order_id) FROM hudi_bxg_ods_oe_order_transfer_apply t  WHERE t.biz_type = 1 AND t.status = 0 AND t.fee_transfer_type=0 AND t.delete_flag = false;





CREATE TABLE if not exists hudi_dwd_oe_order (
    `id` STRING,
    `channel` STRING,
    `student_id` STRING,
    `order_no` STRING,
    `total_amount` DECIMAL(10,2),
    `discount_amount` DECIMAL(10,2),
    `charge_against_amount` DECIMAL(10,2),
    `payable_amount` DECIMAL(10,2),
    `status` INT,
    `pay_status` INT,
    `pay_time` TIMESTAMP(3),
    `paid_amount` DECIMAL(10,2),
    `effective_date` TIMESTAMP(3),
    `terminal` INT,
    `refund_status` INT,
    `refund_amount` DECIMAL(10,2),
    `refund_time` TIMESTAMP(3),
    `create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
    `delete_flag` BOOLEAN,
    `is_target_order` BOOLEAN,
PRIMARY KEY (`id`) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/dwd_oe_order'
    ,'hoodie.datasource.write.recordkey.field'= 'id' 
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000' 
    ,'table.type'= 'MERGE_ON_READ' 
    ,'compaction.async.enabled'= 'true' 
    ,'compaction.trigger.strategy'= 'num_commits' 
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true' 
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true' 
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true' 
    ,'hive_sync.mode'= 'hms' 
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'dwd_oe_order'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);
~~~

###### 拉起数据任务（2个任务）

~~~shell
insert into hudi_dwd_oe_stu_course_order
SELECT
    `osco`.`id`,
    `osco`.`student_course_id`,
    `osco`.`order_id`,
    `osc`.`course_id`,
`osc`.`status` as `stu_course_status`,
case `osc`.`status` when 0 then '试学' when 1 then '生效' when 2 then '待生效' when -1 then '停课' else '退费' end as `stu_course_status_des`,
    `osc`.`delete_flag` as `stu_course_delete_flag`,
`osc`.`effective_date`,
    `oo`.`payable_amount`,
    `oo`.`pay_status`,
    `oo`.`pay_time`,
    `oo`.`paid_amount`,
    `oo`.`refund_status`,
    `oo`.`delete_flag` as `order_delete_flag`,
    `oc`.`grade_name`,
`oc`.`course_type`,
    if (oo.`payable_amount`>0 and `oo`.`pay_status`=2 and `oo`.`delete_flag` = false and `osc`.`delete_flag` = false, true, false) as is_complete_order
FROM hudi_bxg_ods_oe_stu_course_order AS osco
LEFT JOIN hudi_bxg_ods_oe_stu_course AS osc
ON osc.id = osco.student_course_id
LEFT JOIN hudi_bxg_ods_oe_order AS oo
ON oo.id = osco.order_id
LEFT JOIN hudi_bxg_ods_oe_course AS oc
ON oc.id = osc.course_id;







insert into hudi_dwd_oe_order
SELECT
    `id`, `channel`, `student_id`, `order_no`, `total_amount`, `discount_amount`, `charge_against_amount`, `payable_amount`, `status`, `pay_status`, `pay_time`, `paid_amount`, `effective_date`, `terminal`, `refund_status`, `refund_amount`, `refund_time`, `create_time`,`update_time`, `delete_flag`,
if (`ccv`.`target_order_id` is not null, true, false) AS `is_target_order`
FROM hudi_bxg_ods_oe_order AS oo
LEFT JOIN `bxg_common_change_classes_v` AS `ccv`
    ON `oo`.`id`=`ccv`.`target_order_id`;
~~~

###### 校验数据（HDFS、Hive）

* HDFS

![1694524903323](assets/1694524903323.png)

* Hive校验

![1694525102428](assets/1694525102428.png)

![1694525166540](assets/1694525166540.png)

#### Doris DWD层

##### 数据流图

![1694525380472](assets/1694525380472.png)

操作步骤：

~~~shell
在Doris中创建库、表
在FlinkSQL创建Doris的映射表
拉起数据任务
校验数据（Doris）
~~~

###### 在Doris中创建库、表

~~~shell
CREATE TABLE IF NOT EXISTS bxg.dwd_oe_stu_course_order
(
   `id` int,
   `stu_course_id` int COMMENT '学员课程id',
   `order_id` string,
   `course_id` int COMMENT '学员购买的课程',
   `stu_course_status` int COMMENT '学员课程状态：0试学、1生效、2待生效、-1停课、8退费',
`stu_course_status_des` string COMMENT '学员课程状态描述：0试学、1生效、2待生效、-1停课、8退费',
   `stu_course_delete_flag` BOOLEAN,
`effective_date` datetime,
   `payable_amount` decimal(10,2) COMMENT '实际应付总金额=原价-优惠总额-冲抵金额',
   `pay_status` int  COMMENT '支付状态：0未支付、1部分支付、2支付完成',
   `pay_time` datetime COMMENT '最后支付完成时间',
   `paid_amount` decimal(10,2) COMMENT '当前已付总额',
   `refund_status` INT COMMENT '退费状态:0-未退费;-1-已退费;-2-退费中;-3-部分退费',
   `order_delete_flag` BOOLEAN COMMENT 'ods_bxg_oe_order表中订单是否删除',
   `grade_name` string COMMENT '课程名称',
`course_type`  int,
   `is_complete_order` BOOLEAN COMMENT '实际应付总金额0且支付状态pay_status完成'
) Unique Key (`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);




CREATE TABLE  if not exists bxg.`dwd_oe_order` (
    `id` varchar(32) NOT NULL,
    `channel` string NOT NULL COMMENT '订单渠道来源：BXG/博学谷，目前只有博学谷，将来可能会有黑马短训、酷丁鱼等',
    `student_id` string NOT NULL COMMENT '用户ID',
    `order_no` string NOT NULL COMMENT '订单号，生成规则：年（2位）-月（2位）-日（2位）-时（2位）-随机码（12位） eg.16110910aRdK45Y86qe3',
    `total_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '原价/总价',
    `discount_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '优惠总额，有可能是优惠券优惠、也有可能是满减优惠',
    `charge_against_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '冲抵金额，目前包含报名费',
    `payable_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '实际应付总金额=原价-优惠总额-冲抵金额',
    `status` int NOT NULL DEFAULT '0' COMMENT '订单状态：0未生效、1已生效、-1已关闭。和订单支付状态区分开，因为在某些情况下学员没有支付完成订单也已经开始生效。“-1已关闭”状态代表已退费和超时关闭两种含义。',
    `pay_status` int NOT NULL COMMENT '支付状态：0未支付、1部分支付、2支付完成',
    `pay_time` datetime DEFAULT NULL COMMENT '最后支付完成时间',
    `paid_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '当前已付总额',
    `effective_date` datetime DEFAULT NULL COMMENT '订单生效日期。从该日期开始计算服务期。',
    `terminal` int NOT NULL DEFAULT '0' COMMENT '下单订单终端：0/PC官网、1/后台导入-其他、2/App、3/移动官网、4微信内、5/后台导入-线下转线上、6/ios、7/补录-系统-N12分摊转移、8/小程序(在线编程)',
    `refund_status` int NOT NULL DEFAULT '0' COMMENT '退费状态:0-未退费;-1-已退费;-2-退费中;-3-部分退费',
    `refund_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '退费金额',
`refund_time` datetime DEFAULT NULL COMMENT '最后退费时间',
`create_time` datetime NOT NULL COMMENT '物理入库时间，如果是补录订单，该时间为补录订单的日期，而不是学员真实缴费的日期。',
    `update_time` datetime NOT NULL,
`delete_flag` boolean NOT NULL,
`is_target_order` boolean
    )  UNIQUE KEY(`id`)
    COMMENT '订单：主订单'
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
               );
~~~

###### 在FlinkSQL创建Doris的映射表

~~~shell
CREATE TABLE if not exists doris_dwd_oe_stu_course_order (
     `id` int,
     `stu_course_id` int,
     `order_id` string,
     `course_id` int,
     `stu_course_status` int,
`stu_course_status_des` string,
     `stu_course_delete_flag` BOOLEAN,
`effective_date` TIMESTAMP(3),
     `payable_amount` decimal(10,2),
     `pay_status` int,
     `pay_time` TIMESTAMP(3),
     `paid_amount` decimal(10,2),
     `refund_status` int,
     `order_delete_flag` boolean,
     `grade_name` string,
`course_type` INT,
     `is_complete_order` boolean,
     PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'bxg.dwd_oe_stu_course_order'
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





CREATE TABLE if not exists doris_dwd_oe_order (
                                                `id` STRING,
    `channel` STRING,
    `student_id` STRING,
    `order_no` STRING,
    `total_amount` DECIMAL(10,2),
    `discount_amount` DECIMAL(10,2),
    `charge_against_amount` DECIMAL(10,2),
    `payable_amount` DECIMAL(10,2),
    `status` INT,
    `pay_status` INT,
    `pay_time` TIMESTAMP(3),
    `paid_amount` DECIMAL(10,2),
    `effective_date` TIMESTAMP(3),
    `terminal` INT,
    `refund_status` INT,
    `refund_amount` DECIMAL(10,2),
`refund_time` TIMESTAMP(3),
`create_time` TIMESTAMP(3),
    `update_time` TIMESTAMP(3),
`delete_flag` BOOLEAN,
`is_target_order` BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
          'fenodes' = '192.168.88.161:8030'
          ,'table.identifier' = 'bxg.dwd_oe_order'
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

###### 拉起数据任务

~~~shell
INSERT INTO doris_dwd_oe_stu_course_order SELECT `id`,`stu_course_id`, `order_id`,`course_id`,`stu_course_status`,`stu_course_status_des`,`stu_course_delete_flag`, `effective_date`,`payable_amount`,`pay_status`,`pay_time`,`paid_amount`,`refund_status`, `order_delete_flag`, `grade_name`, `course_type`,`is_complete_order`
FROM hudi_dwd_oe_stu_course_order;



INSERT INTO `doris_dwd_oe_order` SELECT  `id`, `channel`, `student_id`, `order_no`, `total_amount`, `discount_amount`, `charge_against_amount`, `payable_amount`, `status`, `pay_status`, `pay_time`, `paid_amount`, `effective_date`, `terminal`, `refund_status`, `refund_amount`, `refund_time`,`create_time`,`update_time`, `delete_flag` , `is_target_order`
FROM hudi_dwd_oe_order;
~~~

###### 校验数据（Doris）

![1694525802460](assets/1694525802460.png)

#### Hudi DWS层

DWS层，属于聚合层，根据最终的结果来进行展示。所以需要根据结果俩进行建表。

结果中除了前2个指标是一行一列的结果之外，其他的结果都是固定的。按照年月分组，求每年每月的指标数。

因此，可以使用一张大的聚合表来满足DWS层指标的计算。

##### 数据流图

![1694526290178](assets/1694526290178.png)

操作步骤：

~~~shell
在FlinkSQL创建Hudi DWS层的映射表
拉起数据任务
校验数据
~~~

###### 在FlinkSQL创建Hudi DWS层的映射表

~~~shell
CREATE TABLE if not exists hudi_dws_overall_revenue_achievement(
course_id INT,
`year` BIGINT,
`mon` BIGINT,
eff_year BIGINT,
eff_mon BIGINT,
course_type int,
`stu_course_delete_flag` BOOLEAN,
`stu_course_status` INT,
 grade_name STRING,
`sm` decimal(38,6),
`cnt` BIGINT,
 PRIMARY KEY (course_id,`year`,`mon`,eff_year,eff_mon,course_type,stu_course_delete_flag,stu_course_status) NOT ENFORCED
) WITH(
    'connector'='hudi'
    ,'path'= 'hdfs://node1:8020/hudi/bxg/dws_overall_revenue_achievement'
    ,'hoodie.datasource.write.recordkey.field'= 'course_id,`year`,`mon`,eff_year,eff_mon,course_type,stu_course_delete_flag,stu_course_status'
    ,'write.tasks'= '1'
    ,'compaction.tasks'= '1'
    ,'write.rate.limit'= '2000'
    ,'table.type'= 'MERGE_ON_READ'
    ,'compaction.async.enabled'= 'true'
    ,'compaction.trigger.strategy'= 'num_commits'
    ,'compaction.delta_commits'= '1'
    ,'changelog.enabled'= 'true'
    ,'read.tasks' = '1'
    ,'read.streaming.enabled'= 'true'
    ,'read.start-commit'='earliest'
    ,'read.streaming.check-interval'= '3'
    ,'hive_sync.enable'= 'true'
    ,'hive_sync.mode'= 'hms'
    ,'hive_sync.metastore.uris'= 'thrift://node1:9083'
    ,'hive_sync.table'= 'dws_overall_revenue_achievement'
    ,'hive_sync.db'= 'bxg'
    ,'hive_sync.username'= ''
    ,'hive_sync.password'= ''
    ,'hive_sync.support_timestamp'= 'true'
);
~~~

###### 拉起数据任务

~~~shell
INSERT INTO hudi_dws_overall_revenue_achievement
SELECT
IFNULL(osco.course_id,-1) as course_id,
IFNULL(year(oo.pay_time),-1)   as  `year`,
IFNULL(month(oo.pay_time),-1) as `mon`,
IFNULL(year(osco.effective_date),-1) as eff_year,
IFNULL(month(osco.effective_date),-1) as eff_mon,
IFNULL(osco.course_type,-1) as course_type,
IFNULL(osco.`stu_course_delete_flag`,FALSE) as stu_course_delete_flag, 
IFNULL(osco.`stu_course_status`,-1) as stu_course_status,
osco.grade_name,
sum(oo.`payable_amount` + (CASE WHEN oo.`charge_against_amount` IS NOT null THEN oo.`charge_against_amount` ELSE 0 END))/ 10000  as `sm`,
count(oo.id)  as `cnt`
FROM
    `hudi_dwd_oe_order` oo
        LEFT JOIN  `hudi_dwd_oe_stu_course_order`  osco
            on  oo.`id` = osco.`order_id`
            WHERE 
-- 支付状态：支付完成
        oo.`pay_status` = 2
-- 未删除订单
  AND oo.`delete_flag` is FALSE 
-- 转班情况只取第一次的订单，转班后的订单不重复计算
  AND oo.`is_target_order` is FALSE 
-- 排除N12分摊转移
  AND oo.`terminal` not in (7)
-- 排除测试课
  AND osco.`course_id` NOT IN (555,1537) 
  GROUP BY year(oo.pay_time),month(oo.pay_time),year(osco.effective_date),month(osco.effective_date),
  osco.course_id,osco.grade_name,
osco.course_type,osco.`stu_course_delete_flag`,osco.`stu_course_status`;
~~~

###### 校验数据

* HDFS校验

![1694526513696](assets/1694526513696.png)

* Hive校验

![1694526603696](assets/1694526603696.png)

#### Doris DWS层

##### 数据流图

![1694526841895](assets/1694526841895.png)

操作步骤：

~~~shell
在Doris创建库、表
在FlinkSQL创建Doris的映射表
拉起数据任务
校验数据（Doris）
~~~

###### 在Doris创建库、表

~~~shell
CREATE TABLE IF NOT EXISTS bxg.dws_overall_revenue_achievement
(
course_id INT,
`year` BIGINT,
`mon` BIGINT,
eff_year BIGINT,
eff_mon BIGINT,
course_type int,
`stu_course_delete_flag` BOOLEAN,
`stu_course_status` INT,
grade_name string,
`sm` decimal(27,6),
`cnt` BIGINT
) Unique Key (course_id,`year`,`mon`,eff_year,eff_mon,course_type,stu_course_delete_flag,stu_course_status)
DISTRIBUTED BY HASH(`course_id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
~~~

###### 在FlinkSQL创建Doris的映射表

~~~shell
CREATE TABLE if not exists doris_dws_overall_revenue_achievement (
course_id INT,
`year` BIGINT,
`mon` BIGINT,
eff_year BIGINT,
eff_mon BIGINT,
course_type int,
`stu_course_delete_flag` BOOLEAN,
`stu_course_status` INT,
grade_name string,
`sm` decimal(27,6),
`cnt` BIGINT,
 PRIMARY KEY (course_id,`year`,`mon`,eff_year,eff_mon,course_type,stu_course_delete_flag,stu_course_status) NOT ENFORCED
) WITH (
    'fenodes' = 'node1:8030'
    ,'table.identifier' = 'bxg.dws_overall_revenue_achievement'
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

###### 拉起数据任务

~~~shell
INSERT INTO `doris_dws_overall_revenue_achievement` SELECT 
course_id,`year`,`mon`,eff_year ,eff_mon ,
course_type,`stu_course_delete_flag`,
`stu_course_status`,grade_name,`sm`,`cnt`
FROM hudi_dws_overall_revenue_achievement;
~~~

###### 校验数据（Doris）

![1694527021700](assets/1694527021700.png)

### 业务查询

#### 年度营收额全款

~~~shell
select sum(sm) from 
bxg.dws_overall_revenue_achievement 
where  `year` = year('2022-1-1');

select sum(sm) from 
bxg.dws_overall_revenue_achievement 
where  `year` = year(current_date());
~~~

#### 年度营收额（进班）

~~~shell
select sum(sm) from 
     bxg.dws_overall_revenue_achievement
where 
`stu_course_delete_flag` = 0 AND
    `stu_course_status` = 1 AND
    `eff_year`= year('2022-1-1');
    
    
select sum(sm) from 
     bxg.dws_overall_revenue_achievement
where 
`stu_course_delete_flag` = 0 AND
    `stu_course_status` = 1 AND
    `eff_year`= year(current_date());
~~~

![1694526476931](assets/1694526476931.png)
