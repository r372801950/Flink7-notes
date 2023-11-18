# Flink基础

## 今日课程内容介绍

* 入门案例
    * Table API（了解）
    * SQL（掌握）

* 提交集群运行

## 入门案例

### 流处理案例 - Table API

#### 需求

~~~shell
使用Flink程序，读取socket的数据源，进行词频统计。采用Table API的方式。
~~~

#### 分析

~~~shell
Table：表，也就是说，它是有结构的。

//1.构建流式执行环境
//Flink有一个专门用来处理表数据的对象，叫做流式表环境对象，它可以使用流式执行环境来构建，StreamTableEnvironment

//2.数据源Source
//指定Source数据源，当然这里的数据源就是表了，也称之Source Table（源表）


//3.数据输出Sink（输出表）



//4.数据处理Transformation
//如果不先定义输出的表，则数据没有目标，也就是说，数据处理完后不知道sink到哪里。



//5.启动流式任务
~~~

#### 实现

~~~java
package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: itcast
 * @date: 2023/8/8 19:57
 * @desc: 需求：使用Flink程序，读取socket的数据源，进行词频统计。采用Table API的方式。
 */
public class Demo01_WordCountTable {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Flink有一个专门用来处理表数据的对象，叫做流式表环境对象，它可以使用流式执行环境来构建，StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //使用流式表环境对象来设置参数
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source
        //指定Source数据源，当然这里的数据源就是表了，也称之Source Table（源表）
        /**
         * createTemporaryTable方法介绍：
         * 参数一：path，表名称。元数据库.数据库.表
         * 参数二：descriptor，表的描述器，它可以描述一张表长什么样子，可以构建一张表
         *
         * TableDescriptor：表的描述器对象
         * forConnector：指定数据源在那里，通过connector（连接器）来连接
         *      Connector：连接器，类似于Java连接mysql的驱动包
         *      socket：表示数据源就是socket
         */
        /**
         * Source表的Schema如下：
         *  |   word    |
         *  |   hello   |
         *  |   hive    |
         *  |   spark   |
         *
         *  socket = hostname + port
         *  所以，我们要额外指定主机名和端口号
         *
         *  source表输入的数据格式format：csv
         *
         *  最后不要忘了build构建
         */
        tEnv.createTemporaryTable("source_table", TableDescriptor.forConnector("socket")
                .schema(Schema.newBuilder()
                        .column("word", DataTypes.STRING()).build())//build是构建Schema
                .option("hostname","node1")
                .option("port","9999").format("csv")
                .build());//build是构建表（包括Schema和connector）

        //3.数据输出Sink（输出表）
        /**
         * Sink表的Schema如下：
         *  |   word    |   counts   |
         *  |   hello   |      2     |
         *  |   hive    |      1     |
         */
        tEnv.createTemporaryTable("sink_table",TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder()
                        .column("word",DataTypes.STRING())
                        .column("counts",DataTypes.BIGINT()).build())//build是构建Schema
                .build());//build是构建表（包括Schema和connector）


        //4.数据处理Transformation
        //如果不先定义输出的表，则数据没有目标，也就是说，数据处理完后不知道sink到哪里。
        /**
         * insert into sink_table
         * select word,count(1) from source_table group by word
         * lit(1):虚拟的列，
         * count：求count操作
         */
        tEnv.from("source_table")//读取source表
                .groupBy(Expressions.$("word"))//对单词进行分组
                .select(Expressions.$("word"),Expressions.lit(1).count())//取单词和单词出现的次数
                .executeInsert("sink_table")//把数据sink到目标表中
                .await();//阻塞执行，等待数据到达


        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1691498932363](assets/1691498932363.png)

### 流处理案例 - SQL

#### 需求

~~~shell
使用Flink程序，读取socket的数据源，进行词频统计。采用SQL的方式来实现。
~~~

#### 分析

~~~shell
//1.构建流式执行环境
//Flink有一个专门用来处理表数据的对象，叫做流式表环境对象，它可以使用流式执行环境来构建，StreamTableEnvironment


//2.数据源Source
//指定Source数据源，当然这里的数据源就是表了，也称之Source Table（源表）


//3.数据输出Sink（输出表）


//4.数据处理Transformation
//如果不先定义输出的表，则数据没有目标，也就是说，数据处理完后不知道sink到哪里。


//5.启动流式任务
~~~

#### 实现

~~~java
package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: itcast
 * @date: 2023/8/8 21:03
 * @desc: 需求：使用Flink程序，读取socket的数据源，进行词频统计。采用SQL的方式来实现。
 */
public class Demo02_WordCountSQL {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Flink有一个专门用来处理表数据的对象，叫做流式表环境对象，它可以使用流式执行环境来构建，StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //设置参数信息，比如：并行度
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source
        //指定Source数据源，当然这里的数据源就是表了，也称之Source Table（源表）
        /**
         *  socket = hostname + port
         *  |   word    |
         *  |   hello   |
         *  |   hive    |
         *  |   spark   |
         *  |   flink   |
         */
        tEnv.createTemporaryTable("source_table", TableDescriptor.forConnector("socket")//数据源是socket数据
                .schema(Schema.newBuilder()
                        .column("word", DataTypes.STRING()).build())//源表的Schema结构
                .option("hostname","node1")//socket的主机名
                .option("port","9999")//socket的端口号
                .format("csv")//源表的数据格式
                .build());

        //3.数据输出Sink（输出表）
        /**
         *  sink表的结构
         *  |   word    |   counts  |
         *  |   hello   |     3     |
         *  |   hive    |     2     |
         */
        tEnv.createTemporaryTable("sink_table", TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder()
                        .column("word",DataTypes.STRING())
                        .column("counts",DataTypes.BIGINT()).build())//构建Schema
                .build());//构建Table

        //4.数据处理Transformation
        //如果不先定义输出的表，则数据没有目标，也就是说，数据处理完后不知道sink到哪里。
        /**
         *  executeSql:可以执行任意的SQL
         *  await:阻塞执行，等待数据到达
         */
        tEnv.executeSql("insert into sink_table" +
                " select word,count(1) from source_table group by word").await();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1691501143616](assets/1691501143616.png)

#### 实现（SQL）

~~~java
package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: itcast
 * @date: 2023/8/8 21:03
 * @desc: 需求：使用Flink程序，读取socket的数据源，进行词频统计。采用SQL的方式来实现。
 */
public class Demo03_WordCountSQL_02 {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Flink有一个专门用来处理表数据的对象，叫做流式表环境对象，它可以使用流式执行环境来构建，StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //设置参数信息，比如：并行度
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source
        //指定Source数据源，当然这里的数据源就是表了，也称之Source Table（源表）
        /**
         *  socket = hostname + port
         *  |   word    |
         *  |   hello   |
         *  |   hive    |
         *  |   spark   |
         *  |   flink   |
         */
        tEnv.executeSql("create table source_table (" +
                "word string" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format' = 'csv'" +
                ")");

        //3.数据输出Sink（输出表）
        /**
         *  sink表的结构
         *  |   word    |   counts  |
         *  |   hello   |     3     |
         *  |   hive    |     2     |
         */
        tEnv.executeSql("create table sink_table (" +
                "word string," +
                "counts bigint" +
                ") with (" +
                "'connector' = 'print'" +
                ")");


        //4.数据处理Transformation
        //如果不先定义输出的表，则数据没有目标，也就是说，数据处理完后不知道sink到哪里。
        /**
         *  executeSql:可以执行任意的SQL
         *  await:阻塞执行，等待数据到达
         */
        tEnv.executeSql("insert into sink_table" +
                " select word,count(1) from source_table group by word").await();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1691501936006](assets/1691501936006.png)

## 提交集群运行

Flink的任务开发好后，需要提交给集群运行，以便最大化利用集群资源，实现分布式运行。

Flink提交集群运行有两种方式：

* 命令行提交
* WebUI提交

不管是哪种运行方式，都必选先打个jar包。然后把jar拿过去用即可。

### 打包

打包之后，会出来2个jar包：这2个jar包一大一小：

~~~shell
#1.大的包
胖包，这个包包含：代码+配置+依赖，也就是说是一个全的jar包，它可以运行在没有Flink集群环境的平台上。


#2.小的包
瘦包，这个包包含：代码+配置，这个包就必须运行在有Flink环境的平台上。
~~~

### 命令行提交

~~~shell
#0.jar包上传的路径
cd /root

#1.命令提交
cd /export/server/flink

#2.拷贝jar包
cp /export/software/flink-examples-table_2.12-1.15.2.jar  ./lib/

#3.开启9999端口
nc -lk 9999

#4.提交运行
bin/flink run -c day03.Demo03_WordCountSQL_02  /root/original-bxg7-1.0-SNAPSHOT.jar

bin/flink run -Dexecution.runtime-mode=STREAMING -m yarn-cluster -yjm 1024 -ytm 1024 -c day03.Demo03_WordCountSQL_02  /root/original-bxg7-1.0-SNAPSHOT.jar

-m yarn-cluster：以per-job模式运行
-yjm：设置JobManager的内存大小
-ytm：设置TaskManager的内存大小


~~~

### WebUI提交

点击：Submit New Job，右上角点击：Add Jar，选择相应的jar包，点击打开即可，如下图所示：

![1691503587715](assets/1691503587715.png)

输入程序入口类和并行度，点击：Submit提交运行。











