# Flink基础

## 今日课程内容介绍

* Flink安装部署
    * Standalone模式
    * Yarn模式
* Flink入门案例
    * 批处理案例
    * 流处理案例
* Flink任务提交

## Flink安装部署

Flink可以部署在多种模式下：

* Local模式（右键run运行就是local模式）
* Standalone模式
* Yarn模式

### local模式

使用一个进程模拟主从角色进行干活。

### Standalone模式

思路：下载 -> 解压 -> 配置 -> 启动集群 -> 提交案例运行

~~~shell
#0.路径
cd /export/software

#1.下载
wget https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz

#2.解压
tar -xf flink-1.15.2-bin-scala_2.12.tgz -C /export/server

#3.配置软连接
cd /export/server
ln -s flink-1.15.2 flink

#4.修改配置文件
cd flink
vim conf/flink-conf.yaml
190行 rest.address: node1
203 rest.bind-address: node1
在最后加上：classloader.check-leaked-classloader: false

#5.启动Flink
bin/start-cluster.sh 

#6.访问
在浏览器地址栏中输入：node1:8081
能看到FlinkWebUI页面即可。

#7.提交任务运行
bin/flink run examples/batch/WordCount.jar

#8.flink命令介绍
bin/flink：可以提交Flink流式应用程序到集群中运行。它是一个客户端脚本。类似于Spark的spark-submit脚本
run：启动一个Flink应用程序
run-application：启动一个Flink应用程序，以Application Mode的方式运行
info：查看信息
list：查看列表
stop：停止任务
cancel：取消任务
savepoint：把任务保存起来，后面再从保存点恢复任务

bin/flink run -p 1 examples/batch/WordCount.jar
bin/flink run -p 2 examples/batch/WordCount.jar
bin/flink run -p 4 examples/batch/WordCount.jar
bin/flink run -p 5 examples/batch/WordCount.jar
结论：并行度的数量一旦超过slot的数量，则任务无法提交运行。
~~~

Flink目录介绍：

![1691302008190](assets/1691302008190.png)

任务运行截图如下：

![1691303528335](assets/1691303528335.png)

### Yarn模式

Flink也是跑在Yarn下。不是使用Flink的集群了。

Flink在Yarn下，有三种运行模式：

* Session模式
* Per-job模式
* Application模式

#### Session模式

Session，翻译为会话，表示一个会话集群。

也就是说，Flink要先开启一个会话集群，其次才能运行Flink任务。

图示如下：

![1691306070295](assets/1691306070295.png)

如果启动了Flink Session集群，**则所有的任务，都运行在这一套集群下**。

优缺点：

能够节省后续任务的初始化时间，也就是可以节省**主节点**的启动时间。

这一套集群资源是共享的，任何Flink任务都是运行在这一套集群资源下。

适用于一些小数据集的任务。

#### Flink如何跑在Yarn上

~~~shell
#1.启动HDFS和Yarn，前提是要启动node2和node3节点
start-all.sh

#2.Flink要不要启动呢？
不要。

#3.添加Flink On Yarn的兼容包
2个包
cp flink-shaded-hadoop-3-uber-3.1.1.7.2.9.0-173-9.0.jar /export/server/flink/lib/
cp commons-cli-1.5.0.jar /export/server/flink/lib/
~~~

Session模式下，需要2步走：

~~~shell
#1.启动Session集群
cd /export/server/flink
bin/yarn-session.sh

#2.提交任务，和Standalone模式一样
bin/flink run examples/batch/WordCount.jar
~~~

运行截图如下：

![1691308159538](assets/1691308159538.png)

### Per-job模式

per-job模式，per：每一个，每一天。job：任务。翻译为：Job分离模式。这个模式和Spark的client模式类似。

每个任务都会在Yarn下创建一套集群，这个集群是运行在Yarn的Container容器里。

任务运行完成后，Container会被销毁，集群也会随着容器的销毁而销毁。

简单理解：和MapReduce任务、Spark任务类似。

![1691310001775](assets/1691310001775.png)

任务脚本：

~~~shell
#1.基础路径
cd /export/server/flink

#2.任务运行脚本
bin/flink run -m yarn-cluster examples/batch/WordCount.jar

-m：指定任务提交给Yarn执行，这个参数表示任务以per-job模式来运行
~~~

任务运行截图如下：

![1691309916646](assets/1691309916646.png)

特点：JobManager：初始化集群后，主节点一直在，知道用户取消集群后，主节点才消失。

Per-job：**主节点和从节点是随着集群的创建而创建，随着集群的销毁而销毁。**

每个任务都有自身的主节点和从节点。互不影响。

### Application模式

Application模式，和Per-job模式类似，区别在于per-job模式，启动的时候，客户端节点在本地。

而Application模式，任务启动的时候，客户端节点在集群中的某一个节点上。

Application模式和Spark的cluster模式类似。

提交命令：

~~~shell
#1.基础路径
cd /export/server/flink

#2.提交任务运行
bin/flink run-application -t yarn-application  examples/batch/WordCount.jar --output hdfs://node1:8020/flink/output5
~~~

截图如下：

![1691310977275](assets/1691310977275.png)

### 小结

Flink On Yarn有三种运行模式：

~~~shell
#1.Session模式
Session模式，会话模式，Flink的所有任务都运行在一个会话集群中。
适合用于一些小数据集的任务。用的不多。
优点：会节省主节点初始化的时间，提升运行效率。
缺点：集群资源是所有任务共享的，如果是大任务，则可能会导致集群挂掉，进而影响其他任务运行。
操作步骤：
（1）启动Session集群
（2）提交任务运行


#2.Per-job模式
per-job模式，Job分离模式，每一个任务都会初始化一个集群，也就是各个任务会初始化各自的主从节点，各自的任务运行，互不影响。
类似于Spark的client模式。适用于一些大数据集的任务。
在早的版本中常用。
优点：各自的任务互不影响，就算有些任务挂了，也不会影响其他任务运行。
缺点：每一个任务都需要经过初始化过程，会消耗一定的初始化时间。
操作步骤：
（1）直接提交任务运行即可，指定 -m yarn-cluster参数即可。


#3.Application模式
Application模式，应用模式，它和per-job模式类似，区别在于Application模式的客户端在集群中的某一个节点上，
而Per-job模式的客户端在本地。类似于Spark的cluster模式。
适用于一些大数据集的任务。在1.15之后的Flink版本中，官方推荐使用Application模式。
在公司中常用。
缺点：每一个任务都需要经过初始化过程，会消耗一定的初始化时间。
优点：各自的任务互不影响，就算有些任务挂了，也不会影响其他任务运行。客户端进行不会在本地启动，避免因本地客户端进程太多导致启动失败的情况。
操作步骤：
（1）直接提交任务运行即可，指定 run-application yarn-application参数即可。
~~~

## Flink入门案例

### 分层API

官方分了4层：

![Programming levels of abstraction](https://nightlies.apache.org/flink/flink-docs-release-1.17/fig/levels_of_abstraction.svg)

或者是3层：

![img](assets/api-stack.png)

官网给Flink的API分了3层：

* 最顶层，高阶API，SQL（SQL）、TableAPI（DSL）

* 核心层，流批统一API，一套代码，既可以做流，也可以做批
* 最底层，状态处理，低阶API，开发中用的不多

### Flink编程模型

~~~shell
#1.初始化流式执行环境

#2.配置数据源Source

#3.数据处理Transformation

#4.数据输出Sink

#5.启动流式任务


//1.构建流式执行环境

//2.数据源Source

//3.数据处理Transformation

//4.数据输出Sink

//5.启动流式任务

~~~

### 官方API介绍

#### 流式执行环境

~~~shell
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
~~~

#### 数据源

~~~shell
#1.基于文件的
readTextFile

#2.基于socket
socketTextStream 

#3.基于集合的
fromElements

#4.自定义数据源
JDBC、Kafka
~~~

#### 数据处理

~~~shell
#1.map
对数据进行一对一转换

#2.flatMap
对数据进行扁平化

#3.filter
对数据进行过滤操作

#4.keyBy
类似于Spark的reduceByKey算子，keyBy算子是Flink中的分流/分组的算子
但注意，只有分流/分组的功能，没有聚合的功能

#5.reduce
对数据进行聚合操作
~~~

#### 数据输出

~~~shell
#1.写文件
writeAsText

#2.标准输出
print

#3.写socket
writeToSocket 

#4.自定义输出
addSink
~~~

#### 启动流式任务

~~~shell
env.execute()
~~~

### 搭建Flink工程

创建一个Maven项目，项目名称为：bxg7，创建好后，把讲义上的依赖拿过来，导入，如下图所示：

![1691313704951](assets/1691313704951.png)

### 入门案例

#### 批处理案例 - DataStream API

##### 需求

~~~shell
对文件中的单词，使用Flink程序，进行词频统计。

hello zookeeper hive
hive spark flink
spark flink flink
hello flink hue
~~~

##### 分析

如下图所示：

![1691321374168](assets/1691321374168.png)

文档分析如下：

~~~shell
#1.数据源
hello zookeeper hive
hive spark flink
spark flink flink
hello flink hue


#2.扁平化，flatMap
hello
zookeeper
hive
hive
spark
flink
spark
flink
flink
hello
flink
hue


#3.转换操作，map
(hello,1)
(zookeeper,1)
(hive,1)
(hive,1)
(spark,1)
(flink,1)
(spark,1)
(flink,1)
(flink,1)
(hello,1)
(flink,1)
(hue,1)


#4.分组/分流，keyBy
(hello,1),(hello,1)
(zookeeper,1)
(hive,1),(hive,1)
(spark,1),(spark,1)
(flink,1),(flink,1),(flink,1),(flink,1)
(hue,1)


#5.聚合，reduce
(hello,2)
(zookeeper,1)
(hive,2)
(spark,2)
(flink,4)
(hue,1)
~~~

##### 实现

~~~java
package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/6 19:31
 * @desc: 需求：对文件中的单词，使用Flink程序，进行词频统计。
 */
public class Demo01_WordCountBatch {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定程序运行的并行度
        env.setParallelism(1);
        //这里手动设置用批的方式来运行
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        //2.数据源Source
        DataStreamSource<String> sourceDS = env.readTextFile("D:\\code\\workspace1\\bxg7\\data\\words.txt");


        //3.数据处理Transformation
        //3.1 flatMap扁平化处理
        /**
         *FlatMapFunction接口，接受2个参数：
         参数一：<T> – Type of the input elements.输入的数据类型，这里就是String类型
         参数二：<O> – Type of the returned elements.返回的数据类型，
            这个由个人自定义，这里返回的是一个一个的单词，因此是String类型
         */
        SingleOutputStreamOperator<String> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            /**
             * flatMap扁平化方法
             * @param value 输入的单词，这里是一行单词
             * @param out 收集器，用来把一个个的单词收集回去
             * @throws Exception 异常信息
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //输入的是一行单词，输出的是一个一个的单词，这里的输入就是value，输出就是out对象（收集器）
                //value:hello zookeeper hive
                String[] words = value.split(" ");
                //对数组中的单词进行迭代，一个个返回
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //3.2 map转换处理
        /**
         * map 算子
         * <T> – Type of the input elements. 输入的数据类型，这里就是String类型
         * <O> – Type of the returned elements.返回的数据类型，这里给Tuple2<String, Integer>，二元组对象，二元组对象需要指定泛型
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            /**
             * map方法
             * @param value 输入的数据,这里就是一个个的单词
             * @return 返回的数据类型
             * @throws Exception 异常信息
             */
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //of方法，可以返回一个Tuple2的对象。它里面就帮我们new一个Tuple2对象
                return Tuple2.of(value, 1);
            }
        });

        //3.3对数据进行分组/分流
        /**
         * KeySelector说明：
         * 参数一：输入的数据类型，这里就是Tuple2
         * 参数二：用来分组/分流的数据类型，这里很显然用的是单词分组，单词是String，因此这里写String
         */
        KeyedStream<Tuple2<String, Integer>, String> keyByDS = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            /**
             * getkey方法，表示用数据中的那个列来进行分组/分流
             * @param value 数据本身
             * @return 指定分组/分流的数据类型
             * @throws Exception 异常信息
             */
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;//f0表示：单词
            }
        });

        //3.4对分组/分流后的数据进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyByDS.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            /**
             * reduce方法，用来聚合
             * @param value1 组内上一个数据
             * @param value2 组内下一个数据
             * @return
             * @throws Exception
             */
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                //value1.f0：单词
                //value1.f1：次数
                //value2.f1：次数
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });


        //4.数据输出Sink
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1691324600748](assets/1691324600748.png)

#### 流处理案例 - DataStream API

##### 需求

~~~shell
使用Flink程序，读取socket中的单词，进行词频统计。
~~~

##### 分析

~~~shell
#1.数据源
socket数据源
socketTextStream()


#2.扁平化，flatMap
hello
zookeeper
hive
hive
spark
flink
spark
flink
flink
hello
flink
hue


#3.转换操作，map
(hello,1)
(zookeeper,1)
(hive,1)
(hive,1)
(spark,1)
(flink,1)
(spark,1)
(flink,1)
(flink,1)
(hello,1)
(flink,1)
(hue,1)


#4.分组/分流，keyBy
(hello,1),(hello,1)
(zookeeper,1)
(hive,1),(hive,1)
(spark,1),(spark,1)
(flink,1),(flink,1),(flink,1),(flink,1)
(hue,1)


#5.聚合，reduce
(hello,2)
(zookeeper,1)
(hive,2)
(spark,2)
(flink,4)
(hue,1)
~~~

##### 实现

~~~java
package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/6 20:47
 * @desc: 需求：使用Flink程序，读取socket中的单词，进行词频统计。
 */
public class Demo02_WordCountStream {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定以流的方式来运行
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.数据源Source
        //socket = hostname + port
        //socketTextStream：读取socket的数据
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 对单词进行扁平化操作
        /**
         * 参数一：一行单词
         * 参数二：扁平化后，就是一个一个的单词
         */
        SingleOutputStreamOperator<String> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            /**
             * flatMap方法，对单词进行切割，然后一个个返回，也就是实现一对多的效果
             * @param value 一行单词
             * @param out 把一个个的单词收集返回
             * @throws Exception 异常信息
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //3.2 对一个个的单词进行map转换操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //返回(单词，1)的形式
                return Tuple2.of(value, 1);
            }
        });

        //3.3 对Tuple2元祖数据进行分组/分流
        KeyedStream<Tuple2<String, Integer>, String> keyByDS = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;//f0就是单词，也就是分组/分流的key
            }
        });

        //3.4 对组内数据进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyByDS.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                //组内的单词，用value1.f0或者value2.f0都一样
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        //4.数据输出Sink
        result.print();

        //5.启动流式任务
        env.execute();

        //todo 一定要开启socket数据源，否则会报连接拒绝的错误。
        /**
         * nc -lk 9999
         */
    }
}
~~~

截图如下：

![1691327304031](assets/1691327304031.png)

##### 实现 - 使用高级聚合函数

~~~shell
package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/6 20:47
 * @desc: 需求：使用Flink程序，读取socket中的单词，进行词频统计。
 */
public class Demo03_WordCountStream_02 {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定以流的方式来运行
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.数据源Source
        //socket = hostname + port
        //socketTextStream：读取socket的数据
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 对单词进行扁平化操作
        /**
         * 参数一：一行单词
         * 参数二：扁平化后，就是一个一个的单词
         */
        SingleOutputStreamOperator<String> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            /**
             * flatMap方法，对单词进行切割，然后一个个返回，也就是实现一对多的效果
             * @param value 一行单词
             * @param out 把一个个的单词收集返回
             * @throws Exception 异常信息
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //3.2 对一个个的单词进行map转换操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //返回(单词，1)的形式
                return Tuple2.of(value, 1);
            }
        });

        //3.3 对Tuple2元祖数据进行分组/分流
        KeyedStream<Tuple2<String, Integer>, String> keyByDS = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;//f0就是单词，也就是分组/分流的key
            }
        });

        //3.4 对组内数据进行聚合操作
        //sum(1)：使用数据中的第二个字段进行求和操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyByDS.sum(1);

        //4.数据输出Sink
        result.print();

        //5.启动流式任务
        env.execute();

        //todo 一定要开启socket数据源，否则会报连接拒绝的错误。
        /**
         * nc -lk 9999
         */
    }
}
~~~

截图如下：

![1691327695905](assets/1691327695905.png)

##### 实现 - 链式编程

~~~java
package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/6 20:47
 * @desc: 需求：使用Flink程序，读取socket中的单词，进行词频统计。
 */
public class Demo04_WordCountStream_03 {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定以流的方式来运行
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.数据源Source
        //socket = hostname + port
        //socketTextStream：读取socket的数据
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 对单词进行扁平化操作
        /**
         * 参数一：一行单词
         * 参数二：扁平化后，就是一个一个的单词
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            /**
             * flatMap方法，对单词进行切割，然后一个个返回，也就是实现一对多的效果
             * @param value 一行单词
             * @param out 把一个个的单词收集返回
             * @throws Exception 异常信息
             */
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //返回(单词，1)的形式
                return Tuple2.of(value, 1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;//f0就是单词，也就是分组/分流的key
            }
        }).sum(1);

        //4.数据输出Sink
        result.print();

        //5.启动流式任务
        env.execute();

        //todo 一定要开启socket数据源，否则会报连接拒绝的错误。
        /**
         * nc -lk 9999
         */
    }
}
~~~

截图如下：

![1691327973492](assets/1691327973492.png)

##### Lambda表达式

~~~java
package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/6 20:47
 * @desc: 需求：使用Flink程序，读取socket中的单词，进行词频统计。
 * lambda表达式写法。
 */
public class Demo05_WordCountStream_04 {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定以流的方式来运行
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.数据源Source
        //socket = hostname + port
        //socketTextStream：读取socket的数据
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 对单词进行扁平化操作
        /**
         * 参数一：一行单词
         * 参数二：扁平化后，就是一个一个的单词
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sourceDS.flatMap((String value, Collector<String> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING)//Flink在处理过程中会把类似擦除掉，所以在最后返回时要显示指定返回的数据类型
                .map((String value) -> {
            return Tuple2.of(value, 1);
        }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((Tuple2<String, Integer> value) -> {
            return value.f0;//f0就是单词，也就是分组/分流的key
        }).sum(1);

        //4.数据输出Sink
        result.print();

        //5.启动流式任务
        env.execute();

        //todo 一定要开启socket数据源，否则会报连接拒绝的错误。
        /**
         * nc -lk 9999
         */
    }
}
~~~

截图如下：

![1691329351629](assets/1691329351629.png)

##### Lambda最终版

~~~java
package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author: itcast
 * @date: 2023/8/6 20:47
 * @desc: 需求：使用Flink程序，读取socket中的单词，进行词频统计。
 * lambda表达式写法，最终版。
 */
public class Demo06_WordCountStream_05 {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定以流的方式来运行
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.数据源Source
        //socket = hostname + port
        //socketTextStream：读取socket的数据
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 对单词进行扁平化操作
        /**
         * 参数一：一行单词
         * 参数二：扁平化后，就是一个一个的单词
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sourceDS.flatMap((String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)).returns(Types.STRING)//Flink在处理过程中会把类似擦除掉，所以在最后返回时要显示指定返回的数据类型
                .map(value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        //4.数据输出Sink
        result.print();

        //5.启动流式任务
        env.execute();

        //todo 一定要开启socket数据源，否则会报连接拒绝的错误。
        /**
         * nc -lk 9999
         */
    }
}
~~~

截图如下：

![1691330181975](assets/1691330181975.png)

#### 流处理案例 - Table API

#### 流处理案例 - SQL





















