# Flink项目

## 今晚内容介绍

* Dinky
    * 为什么学Dinky
    * Dinky介绍
    * Dinky使用
    * Dinky整库入湖

大数据技术栈，18罗汉。

## 为什么学Dinky

这个问题，等价于，按照原生的FlinkSQL来开发流式任务，是否方便？

肯定不方便。

能不能这些SQL都帮我们保存起来。

8081页面操作起来不是很方便，只能对任务进行取消操作。

以上这写都是非核心因素。

假设：业务库和表非常多，每一个业务表都需要一个FlinkCDC任务来进行同步。

这就会造成非常多slot资源被ODS层的数据同步任务消耗。能否缩减一下ODS层的任务使用资源呢？

综合来说，有如下2点因素，不方便线上部署操作：

* 任务的监控、告警等
* 数据同步资源占用过高

上述两点因素，导致使用原生的部署方式，效率有点低，而且也不方便后期的运维监控，于是，我们就引入了Dinky。

Dinky，就是为了解决上述2个问题而来的。

## Dinky介绍

### 概述

Dinky是一个国内的公司研发的，而且也不属于Apache的开源项目。也不是国内大厂研发的。

Dinky官网：www.dlink.top

Dinky 是一个开箱即用的一站式实时计算平台，以 Apache Flink 为基础，连接 OLAP 和数据湖等众多框架，

致力于流批一体和湖仓一体的建设与实践。

简单理解：Dinky只能部署FlinkSQL任务。

### 特点

- 沉浸式 FlinkSQL 数据开发：自动提示补全、语法高亮、语句美化、在线调试、语法校验、执行计划、MetaStore、血缘分析、版本对比等
- 支持 FlinkSQL 多版本开发及多种执行模式：Local、Standalone、Yarn/Kubernetes Session、Yarn Per-Job、Yarn/Kubernetes Application
- 支持 Apache Flink 生态：Connector、FlinkCDC、Table Store 等
- 支持 FlinkSQL 语法增强：表值聚合函数、全局变量、执行环境、语句合并、整库同步、共享会话等
- 支持 FlinkCDC 整库实时入仓入湖、多库输出、自动建表
- 支持 SQL 作业开发：ClickHouse、Doris、Hive、Mysql、Oracle、Phoenix、PostgreSql、SqlServer、StarRocks 等
- 支持实时在线调试预览 Table、 ChangeLog、统计图和 UDF
- 支持 Flink Catalog、数据源元数据在线查询及管理
- 支持实时任务运维：上线下线、作业信息、集群信息、作业快照、异常信息、数据地图、数据探查、历史版本、报警记录等
- 支持作为多版本 FlinkSQL Server 以及 OpenApi 的能力
- 支持实时作业报警及报警组：钉钉、微信企业号、飞书、邮箱等
- 支持自动托管的 SavePoint/CheckPoint 恢复及触发机制：最近一次、最早一次、指定一次等
- 支持多种资源管理：集群实例、集群配置、Jar、数据源、报警组、报警实例、文档、用户、系统配置等
- 更多隐藏功能等待小伙伴们探索

### 名字来源

Dinky，早期的名字叫Dlink，

1.Dinky 英译为 “ 小巧而精致的 ” ，最直观的表明了它的特征：轻量级但又具备复杂的大数据开发能力。

2.为 “ Data Integrate No Knotty ” 的首字母组合，英译 “ 数据整合不难 ”，寓意 “ 易于建设批流一体平台及应用 ”。

3.从 Dlink 改名为 Dinky 过渡平滑，更加形象的阐明了开源项目的目标，始终指引参与者们 “不忘初心，方得始终 ”。

### 架构

![1694692867508](assets/1694692867508.png)

图上的Interceptor和Executor等一些概念，是Dinky自己提出来的，这一块也不需要记忆。

## 安装部署

### 下载

Dinky的版本，最新版为0.7.4，项目中用的是0.6.6的版本。这是去年下半年发布的版本。

下载链接：http://www.dlink.top/download/dinky-0.6.6

### 配置

配置MySQL

~~~shell
#登录mysql
mysql -uroot -p123456

#创建数据库
mysql>create database dlink;

#授权
mysql>grant all privileges on dlink.* to 'dlink'@'%' identified by 'dlink' with grant option;
mysql>flush privileges;

#此处用 dlink 用户登录
mysql -udlink -pdlink
~~~

### 初始化数据库

~~~shell
#1.切换数据库
use dlink;

#2.source命令，加载外部SQL脚本
source /export/server/dlink/sql/dlink.sql
~~~

### 配置MySQL连接

~~~shell
#1.编辑application.yml文件
vim /export/server/dlink/config/application.yml

#2.修改MySQL的连接串
url：
username：
password：
driver-class-name：
~~~

### 配置NGINX（可选）

~~~shell
server {
        listen       12000;
        #listen       80;
        #listen       [::]:80;
        #server_name  _;
        server_name  node1;
        root         /export/server/dlink/html;

        # Load configuration files for the default server block.
        include /etc/nginx/default.d/*.conf;

        location / {
            root   /export/server/dlink/html;
            index  index.html index.htm;
                        try_files $uri $uri/ /index.html;
        }

        error_page 404 /404.html;
        location = /404.html {
        }

        error_page 500 502 503 504 /50x.html;
        location = /50x.html {
        }

        location ^~ /api {
            proxy_pass http://node1:8888;
            proxy_set_header   X-Forwarded-Proto $scheme;
            proxy_set_header   X-Real-IP         $remote_addr;
        }
    }

~~~

代理中，配置的前端访问端口为12000，把12000转发到8888端口上。后续我们访问12000端口即可。

### 启动Dinky

~~~shell
#0.路径
cd /export/server/dlink

#1.启动
sh auto.sh start

#2.停止
sh auto.sh stop

#3.重启
sh auto.sh restart

#4.查看状态
sh auto.sh status
~~~

### 访问

~~~shell
http://node1:12000
~~~

默认用户名/密码: admin/admin

截图如下：

![1694694167871](assets/1694694167871.png)

输入用户名和密码：能看到如下页面，说明登录成功：

![1694694255588](assets/1694694255588.png)

## Dinky的使用

### 菜单介绍

![1694696352279](assets/1694696352279.png)

### 配置集群

点击注册中心-> 集群管理 -> 新建按钮，填写集群的相关信息，如下图：

![1694696477698](assets/1694696477698.png)

创建作业：

![1694696794346](assets/1694696794346.png)

数据开发功能按钮说明：

![1694697035835](assets/1694697035835.png)

配置任务：

![1694697445289](assets/1694697445289.png)

### 入门案例

~~~sql
-- 传统方式
-- 设置checkpoint
SET execution.checkpointing.interval=5sec;
-- 创建映射表
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
    ,'path'= 'hdfs://node1:8020/hudi/bxg/ods_oe_course_dlink'
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
    ,'hive_sync.table'= 'ods_oe_course_dlink'
    ,'hive_sync.db'= 'bxg' 
    ,'hive_sync.username'= '' 
    ,'hive_sync.password'= '' 
    ,'hive_sync.support_timestamp'= 'true' 
);
INSERT INTO `hudi_bxg_ods_oe_course`
select  id, grade_name, bigimg_path, video_url, img_alt, description, detailimg_path, smallimg_path, sort, status, learnd_count, learnd_count_flag, original_cost, current_price, course_length, menu_id, is_free, course_detail, course_detail_mobile, course_detail1, course_detail1_mobile, course_plan_detail, course_plan_detail_mobile, course_detail2, course_detail2_mobile, course_outline, common_problem, common_problem_mobile, lecturer_id, is_recommend, recommend_sort, qqno, description_show, rec_img_path, pv, course_type, default_student_count, study_status, online_course, course_level, content_type, recommend_type, employment_rate, employment_salary, score, cover_url, offline_course_url, outline_url, project_page_url, preschool_test_flag, service_period, included_validity_period, validity_period, qualified_jobs, work_year_min, work_year_max, promote_flag, create_person, update_person, create_time, update_time, is_delete
from `mysql_bxg_oe_course`;
~~~

截图如下：

![1694698086195](assets/1694698086195.png)

结论：把之前在FlinkSQL中执行的SQL都挪到Dinky中执行即可。

## 整库入湖

### 背景

目前通过 FlinkCDC 进行会存在诸多问题，如需要定义大量的 DDL 和编写大量的 INSERT INTO，更为严重的是会占用大量的数据库连接，对 Mysql 和网络造成压力。

Dinky 定义了 CDCSOURCE 整库同步的语法，该语法和 CDAS 作用相似，可以直接自动构建一个整库入仓入湖的实时任务，并且对 source 进行了合并，不会产生额外的 Mysql 及网络压力，支持对任意 sink 的同步，如
kafka、doris、hudi、jdbc 等等

### 语法

~~~shell
EXECUTE CDCSOURCE jobname 
  WITH ( key1=val1, key2=val2, ...)
~~~

官网的配置项有如下说明：

**配置项**

| 配置项            | 是否必须 | 默认值        | 说明                                                         |
| ----------------- | -------- | ------------- | ------------------------------------------------------------ |
| connector         | 是       | 无            | 指定要使用的连接器，当前支持 mysql-cdc 及 oracle-cdc         |
| hostname          | 是       | 无            | 数据库服务器的 IP 地址或主机名                               |
| port              | 是       | 无            | 数据库服务器的端口号                                         |
| username          | 是       | 无            | 连接到数据库服务器时要使用的数据库的用户名                   |
| password          | 是       | 无            | 连接到数据库服务器时要使用的数据库的密码                     |
| scan.startup.mode | 否       | latest-offset | 消费者的可选启动模式，有效枚举为“initial”和“latest-offset”   |
| database-name     | 否       | 无            | 如果table-name="test\.student,test\.score",此参数可选。      |
| table-name        | 否       | 无            | 支持正则,示例:"test\.student,test\.score"                    |
| source.*          | 否       | 无            | 指定个性化的 CDC 配置，如 source.server-time-zone 即为 server-time-zone 配置参数。 |
| checkpoint        | 否       | 无            | 单位 ms                                                      |
| parallelism       | 否       | 无            | 任务并行度                                                   |
| sink.connector    | 是       | 无            | 指定 sink 的类型，如 datastream-kafka、datastream-doris、datastream-hudi、kafka、doris、hudi、jdbc 等等，以 datastream- 开头的为 DataStream 的实现方式 |
| sink.sink.db      | 否       | 无            | 目标数据源的库名，不指定时默认使用源数据源的库名             |
| sink.table.prefix | 否       | 无            | 目标表的表名前缀，如 ODS *即为所有的表名前拼接 ODS*          |
| sink.table.suffix | 否       | 无            | 目标表的表名后缀                                             |
| sink.table.upper  | 否       | 无            | 目标表的表名全大写                                           |
| sink.table.lower  | 否       | 无            | 目标表的表名全小写                                           |
| sink.*            | 否       | 无            | 目标数据源的配置信息，同 FlinkSQL，使用 ${schemaName} 和 ${tableName} 可注入经过处理的源表名 |

### 示例

~~~shell
--cdcsource方式，同步两张表至hudi
execute cdcsource demo with (
'connector' = 'mysql-cdc',
'hostname' = 'node1',
'port' = '3306',
'username' = 'root',
'password' = '123456',
'source.server-time-zone' = 'Asia/Shanghai',
'checkpoint'='5000',
'scan.startup.mode'='initial',
'parallelism'='1',
'database-name'='bxg',
'table-name'='bxg\.oe_test_course,bxg\.oe_surveys_subjects',
'sink.connector'='hudi',
'sink.path'='hdfs://node1:8020/hudi/bxg/${tableName}',
'sink.hoodie.datasource.write.recordkey.field'='id',
'sink.hoodie.parquet.max.file.size'='268435456',
'sink.write.precombine.field'='update_time',
'sink.write.tasks'='1',
'sink.write.bucket_assign.tasks'='2',
'sink.write.precombine'='true',
'sink.compaction.async.enabled'='true',
'sink.write.task.max.size'='1024',
'sink.write.rate.limit'='3000',
'sink.write.operation'='upsert',
'sink.table.type'='COPY_ON_WRITE',
'sink.compaction.tasks'='1',
'sink.compaction.delta_seconds'='20',
'sink.compaction.async.enabled'='true',
'sink.read.streaming.skip_compaction'='true',
'sink.compaction.delta_commits'='20',
'sink.compaction.trigger.strategy'='num_or_time',
'sink.compaction.max_memory'='500',
'sink.changelog.enabled'='true',
'sink.read.streaming.enabled'='true',
'sink.read.streaming.check.interval'='3',
'sink.hive_sync.enable'='true',
'sink.hive_sync.mode'='hms',
'sink.hive_sync.db'='bxg',
'sink.hive_sync.table'='${tableName}',
'sink.table.prefix.schema'='true',
'sink.hive_sync.metastore.uris'='thrift://node1:9083',
'sink.hive_sync.username'=''
)
~~~

截图如下：

![1694699373776](assets/1694699373776.png)

### 整库同步案例

根据上面的案例中，只需要把表修改即可，想同步多少张就同步多少张。

~~~shell
EXECUTE CDCSOURCE jobname WITH (
'connector' = 'mysql-cdc',
'hostname' = '192.168.88.161',
'port' = '3306',
'username' = 'root',
'password' = '123456',
'source.server-time-zone' = 'Asia/Shanghai',
'checkpoint'='90000',
'scan.startup.mode'='initial',
'parallelism'='2',
'database-name'='bxg',
'table-name'='bxg\.oe_course,bxg\.oe_stu_course,bxg\.oe_stu_course_order,bxg\.oe_order,bxg\.oe_order_transfer_apply,
bxg\.oe_user,bxg\.oe_programming_course,bxg\.oe_stu_programming_learning_history,bxg\.oe_order_refund,bxg\.oe_order_refund_apply',
'sink.connector'='hudi',
'sink.path'='hdfs://192.168.88.161:8020/hudi/bxg_dlink/${tableName}',
'sink.hoodie.datasource.write.recordkey.field'='id',
'sink.hoodie.parquet.max.file.size'='268435456',
'sink.write.precombine.field'='update_time',
'sink.write.tasks'='1',
'sink.write.bucket_assign.tasks'='2',
'sink.write.precombine'='true',
'sink.compaction.async.enabled'='true',
'sink.write.task.max.size'='1024',
'sink.write.rate.limit'='3000',
'sink.write.operation'='upsert',
'sink.table.type'='MERGE_ON_READ',
'sink.compaction.tasks'='1',
'sink.compaction.delta_seconds'='20',
'sink.compaction.async.enabled'='true',
'sink.read.streaming.skip_compaction'='true',
'sink.compaction.delta_commits'='20',
'sink.compaction.trigger.strategy'='num_or_time',
'sink.compaction.max_memory'='500',
'sink.changelog.enabled'='true',
'sink.read.streaming.enabled'='true',
'sink.read.streaming.check.interval'='3',
'sink.hive_sync.enable'='true',
'sink.hive_sync.mode'='hms',
'sink.hive_sync.db'='bxg_dlink',
'sink.hive_sync.table'='${tableName}',
'sink.table.prefix'='hudi_bxg_ods_',
'sink.hive_sync.metastore.uris'='thrift://192.168.88.161:9083',
'sink.hive_sync.username'=''
)
~~~

## 项目开发

ODS：整库同步。已经搞定了。

DWD：直接把FlinkSQL中执行的代码挪到Dinky上即可。

DWS：直接把FlinkSQL中执行的代码挪到Dinky上即可。

这就是部署上线了。







