# Flink基础

## 今日课程内容介绍

* UDF自定义函数
    * UDF
    * UDTF
    * UDAF
    * UDTAF
* SQL优化
* Flink项目
    * 背景
    * 架构（重点）
    * 非功能描述
    * Flink整合Hive（永久表、catalog）
    * FlinkCDC

## UDF

### 概述

UDF，User-define Function，用户自定义函数。

Flink官网上有函数的介绍：模糊函数、精确函数。

~~~shell
#1.模糊函数
select count(1) from table;


#2.精确函数
select catalog_name.db_name.function_name from table;
~~~

内置函数：https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/systemfunctions/

内置函数和MySQL、Hive、Spark的内置函数大部分都相似。如果碰到陌生函数，可以在官网上查找。

接下来的重点是UDF（用户自定义函数）。

UDF：https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/udfs/

### UDF分类

Flink的UDF函数可以分为四类：

* UDF，一进一出的函数，Scalar Function（标量函数）

* UDTF，一进多出的函数，Table Function（表值函数），类似于Hive中的explode

* UDAF，多进一出的函数，Aggregate Function（聚合函数）
* UDTAF，多进多出的函数，Table Aggregate Function（表值聚合函数）

### 演示

#### UDF（Scalar Function）

##### 需求

~~~shell
实现两数之和的自定义函数，函数名为mySum
~~~

##### 分析

~~~shell
//1.构建流式执行环境
//构建流式表环境

//2.数据源Source表

//3.数据输出Sink表

//4.数据处理Transformation
//4.1 创建并注册函数到Flink环境中
//2.2 在SQL中使用函数

//5.启动流式任务
~~~

##### 实现

~~~java
package day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author: itcast
 * @date: 2023/8/26 13:55
 * @desc: 实现两数之和的自定义函数，函数名为mySum
 */
public class Demo01_ScalarFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建流式表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source表
        /**
         * Source的Schema如下
         *  |   num1    |   num2    |
         *  |    1      |    2      |
         *  |    3      |    5      |
         */
        tEnv.executeSql("create table source_table (" +
                "num1 bigint," +
                "num2 bigint" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format' = 'csv'" +
                ")");

        //3.数据输出Sink表
        /**
         * Sink表的Schema如下：
         *  |   num   |
         *  |    3    |
         *  |    8    |
         */
        tEnv.executeSql("create table sink_table (" +
                "num bigint" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理Transformation
        //4.1 创建并注册函数到Flink环境中
        //createTemporaryFunction：创建临时函数
        tEnv.createTemporaryFunction("mySum",MyScalarFunction.class);
        //2.2 在SQL中使用函数
        tEnv.executeSql("insert into sink_table select mySum(num1,num2) from source_table").await();

        //5.启动流式任务
        env.execute();
    }

    /**
     * 自定义的类，用来实现自定义UDF函数，它必须继承自ScalarFunction类，至少实现一个eval方法
     */
    public static class MyScalarFunction extends ScalarFunction {

        /**
         * eval方法
         * @param a 第一个数
         * @param b 第二个数
         * @return a+b的和
         */
        public Long eval(Long a, Long b) {
            return a + b;
        }
        public Integer eval(Integer a, Integer b) {
            return a + b;
        }
    }
}


~~~

截图如下：

![1693031273556](assets/1693031273556.png)

#### UDTF（Table Function）

##### 需求

~~~shell
实现一个类似于flatMap功能，输出多个数，函数名myFlatMap
~~~

##### 分析

~~~shell
//1.构建流式执行环境
//构建流式表环境

//2.数据源Source表

//3.数据输出Sink表

//4.数据处理Transformation
//4.1 创建并注册函数到Flink环境中
//2.2 在SQL中使用函数

//5.启动流式任务


一进：3
多出：0,1,2

一进：5
多出：0,1,2,3,4
~~~

##### 实现

~~~java
package day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author: itcast
 * @date: 2023/8/26 14:54
 * @desc: 实现一个类似于flatMap功能，输出多个数，函数名myFlatMap
 */
public class Demo02_TableFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建流式表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source表
        /**
         *  |   num    |
         *  |    3     | -> 0,1,2
         *  |    5     | -> 0,1,2,3,4
         */
        tEnv.executeSql("create table source_table (" +
                "num int" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format'= 'csv'" +
                ")");

        //3.数据输出Sink表
        /**
         *  |   num    |
         *  |    0     |
         *  |    1     |
         *  |    2     |
         *  |    0     |
         *  |    1     |
         *  |    2     |
         *  |    3     |
         *  |    4     |
         */
        tEnv.executeSql("create table sink_table (" +
                "num int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理Transformation
        //4.1 创建并注册函数到Flink环境中
        tEnv.createTemporaryFunction("myFlatMap",MyTableFunction.class);
        //2.2 在SQL中使用函数
        tEnv.executeSql("insert into sink_table " +
                "select x from source_table left join lateral table(myFlatMap(num)) as tmp(x) on true").await();


        //5.启动流式任务
        env.execute();

    }

    /**
     * 自定义的类，必须继承TableFunction类，实现一个或者多个eval方法
     * 这就是自定义的Table Function，泛型就是输出数据类型，这里输出的数据是整数，因此是整形
     */
    public static class MyTableFunction extends TableFunction<Integer> {
        /**
         * 自定义的eval方法
         * @param num 输入的数据
         */
        public void eval(Integer num) {
            for (int i = 0; i < num; i ++) {
                collect(i);
            }
        }
    }
}
~~~

截图如下：

![1693033994817](assets/1693033994817.png)

#### UDAF（Aggregate Function）

##### 需求

~~~shell
实现词频统计案例，使用自定义count函数实现，函数名为myCount
~~~

##### 分析

~~~shell
//1.构建流式执行环境
//构建流式表环境

//2.数据源Source表

//3.数据输出Sink表

//4.数据处理Transformation
//4.1 创建并注册函数到Flink环境中
//2.2 在SQL中使用函数

//5.启动流式任务

~~~

##### 实现

~~~java
package day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author: itcast
 * @date: 2023/8/26 15:15
 * @desc: 实现词频统计案例，使用自定义count函数实现，函数名为myCount
 */
public class Demo03_AggregateFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建流式表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source表
        /**
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

        //3.数据输出Sink表
        /**
         *  |   word    |   counts   |
         *  |   hello   |      2     |
         *  |   hive    |      1     |
         *  |   spark   |      3     |
         *  |   flink   |      2     |
         */
        tEnv.executeSql("create table sink_table (" +
                "word string, " +
                "counts int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理Transformation
        //4.1 创建并注册函数到Flink环境中
        tEnv.createTemporaryFunction("myCount",MyAggregateFunction.class);
        //2.2 在SQL中使用函数
        tEnv.executeSql("insert into sink_table select word,myCount(1) from source_table group by word").await();


        //5.启动流式任务
        env.execute();
    }

    /**
     * 自定义的聚合函数类，必须继承AggregateFunction类，实现如下3个方法：
     * createAccumulator：创建累加器，其实就是一个类
     * accumulate：累加计算的方法，实现中间数据的累加计算
     * getValue：获取最终结果
     * 其中，AggregateFunction需要接受2个泛型：
     * 泛型一：最终的返回结果，这里就是整数
     * 泛型二：中间累加计算的结果类型，也就是自定义的累加器的类
     */
    public static class MyAggregateFunction extends AggregateFunction<Integer,MyAccumulator> {

        /**
         * 创建累加器的方法
         * @return 创建好的累加器
         */
        @Override
        public MyAccumulator createAccumulator() {
            return new MyAccumulator();
        }

        /**
         * 累加计算的方法
         * @param accumulator 累加器对象，每一次累加计算后，结果都应该更新回去
         * @param num 每一次数据进来的词频，这里就是1
         */
        public void accumulate(MyAccumulator accumulator, Integer num) {
            accumulator.count = accumulator.count + 1;
        }

        /**
         * 统计最终结果的方法
         * @param accumulator 累加器对象
         * @return 最终的词频结果
         */
        @Override
        public Integer getValue(MyAccumulator accumulator) {
            return accumulator.count;
        }
    }

    /**
     * 自定义的累加器的类
     * 类的内容和最终的结果相关，由于这里最终的结果是一个整数，因此可以创建一个临时的整数变量，用来保存最终结果
     */
    public static class MyAccumulator {
        public Integer count = 0;
    }
}
~~~

截图如下：

![1693036048351](assets/1693036048351.png)

#### UDTAF（Table Aggregate Function）

##### 需求

~~~shell
取词频统计结果中的Top2，使用自定义函数实现，函数名为top2
~~~

##### 分析

~~~shell
#1.实现思路
//1.构建流式执行环境
//构建流式表环境

//2.数据源Source表

//3.数据输出Sink表

//4.数据处理Transformation
//4.1 创建并注册函数到Flink环境中
//2.2 UDTAF函数，得使用Table API来实现，借助于flatAggregate方法来实现多进多出。

//5.启动流式任务



#2.实现逻辑
hello,1					->				(hello,1)

hello,1									(hello,5)
hello,5					->				(hello,1)

hello,1
hello,5					-> 				(hello,8)
hello,8									(hello,5)

hello,1
hello,5					->				(hello,8)
hello,8									(hello,6)
hello,6
~~~

##### 实现

~~~java
package day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/26 16:15
 * @desc: 取词频统计结果中的Top2，使用自定义函数实现，函数名为top2
 */
public class Demo04_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建流式表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("parallelism.default","1");

        //2.数据源Source表
        /**
         *  |   word    |   counts   |
         *  |   hello   |     3      |  -> (hello,3)
         *  |   hello   |     2      |  -> (hello,3),(hello,2)
         *  |   hello   |     5      |  -> (hello,5),(hello,3)
         */
        tEnv.executeSql("create table source_table (" +
                "word string," +
                "counts int" +
                ") with (" +
                "'connector' = 'socket'," +
                "'hostname' = 'node1'," +
                "'port' = '9999'," +
                "'format' = 'csv'" +
                ")");

        //3.数据输出Sink表
        /**
         *  |   word    |   counts   |
         *  |   hello   |     3      |
         *  |   hello   |     3      |
         *  |   hello   |     2      |
         *  |   hello   |     5      |
         *  |   hello   |     3      |
         */
        tEnv.executeSql("create table sink_table (" +
                "word string, " +
                "counts int" +
                ") with (" +
                "'connector' = 'print'" +
                ")");

        //4.数据处理Transformation
        //4.1 创建并注册函数到Flink环境中
        tEnv.createTemporaryFunction("top2",MyTableAggregateFunction.class);
        //2.2 UDTAF函数，得使用Table API来实现，借助于flatAggregate方法来实现多进多出，最好给个别名，否则默认的名称为EXPR$0
        tEnv.from("source_table").groupBy(Expressions.$("word"))
                //cnt就是top2函数作用在counts列后的结果别名
                .flatAggregate(Expressions.call("top2",Expressions.$("counts")).as("cnt"))
                .select(Expressions.$("word"),Expressions.$("cnt"))
                .executeInsert("sink_table")
                .await();

        //5.启动流式任务
        env.execute();
    }

    /**
     * 自定义的表值聚合函数的类，用来实现UDTAF函数，它的泛型有两个：
     * 泛型一：最终的结果类型，这里就是整数
     * 泛型二：中间累加过程中的结果类型，这个就是自定义累加器类型
     */
    public static class MyTableAggregateFunction extends TableAggregateFunction<Integer,MyAccumulator> {
        /**
         * 创建累加器
         * @return 创建好后的累加器对象
         */
        @Override
        public MyAccumulator createAccumulator() {
            return new MyAccumulator();
        }

        /**
         * 累加计算的方法
         * @param accumulator 累加器，每一次累加计算后，都需要把结果更新回去
         * @param num 当前的word出现的次数
         */
        public void accumulate(MyAccumulator accumulator, Integer num) {
            //当num超过类加器中的firstValue的值后，则需要把num保存下来，赋值给firstValue
            if (accumulator.firstValue < num) {
                //如果num大于最大的值，则需要更新最大值和第二大的值
                //一定要先把firstValue赋值给secondValue，否则secondValue永远都无法更新了，也达不到需求了
                accumulator.secondValue = accumulator.firstValue;
                accumulator.firstValue = num;
            } else if (accumulator.secondValue < num) {
                //如果num大于第二大的值，则更新第二大的值
                accumulator.secondValue = num;
            }
        }

        /**
         * 返回最终的累加结果
         * @param accumulator
         * @param out
         */
        public void emitValue(MyAccumulator accumulator, Collector<Integer> out) {
            if (accumulator.firstValue > Integer.MIN_VALUE) {
                //如果firstValue大于Integer.MIN_VALUE，则说明firstValue一定有输入的值。所以这会儿可以输出。
                out.collect(accumulator.firstValue);
            }
            if (accumulator.secondValue > Integer.MIN_VALUE) {
                //如果secondValue大于Integer.MIN_VALUE，则说明secondValue一定有输入的值。所以这会儿可以输出。
                out.collect(accumulator.secondValue);
            }
        }
    }

    /**
     * 自定义的累加器的类，用来实现top2的结果保存
     */
    public static class MyAccumulator {
        public Integer firstValue = Integer.MIN_VALUE;//用来保存结果中的最大值
        public Integer secondValue = Integer.MIN_VALUE;//用来保存结果中的第二大的值
    }
}
~~~

截图如下：

![1693040516495](assets/1693040516495.png)

## SQL优化

SQL的优化参数从如下三个层面来进行：

* 运行时参数
* 优化器参数

* 表参数

### 运行时参数

* 调整异步IO的缓存数据量

~~~shell
# 默认值：100
# 值类型：Integer
# 流批任务：流、批任务都支持
# 用处：异步 lookup join 中最大的异步 IO 执行数目
table.exec.async-lookup.buffer-capacity: 100
~~~

* 开启微批

~~~shell
# 默认值：false
# 值类型：Boolean
# 流批任务：流任务支持
# 用处：MiniBatch 优化是一种专门针对 unbounded 流任务的优化（即非窗口类应用），其机制是在 `允许的延迟时间间隔内` 以及 `达到最大缓冲记录数` 时触发以减少 `状态访问` 的优化，从而节约处理时间。下面两个参数一个代表 `允许的延迟时间间隔`，另一个代表 `达到最大缓冲记录数`。
table.exec.mini-batch.enabled: false

# 默认值：0 ms
# 值类型：Duration
# 流批任务：流任务支持
# 用处：此参数设置为多少就代表 MiniBatch 机制最大允许的延迟时间。注意这个参数要配合 `table.exec.mini-batch.enabled` 为 true 时使用，而且必须大于 0 ms
table.exec.mini-batch.allow-latency: 0 ms

# 默认值：-1
# 值类型：Long
# 流批任务：流任务支持
# 用处：此参数设置为多少就代表 MiniBatch 机制最大缓冲记录数。注意这个参数要配合 `table.exec.mini-batch.enabled` 为 true 时使用，而且必须大于 0
table.exec.mini-batch.size: -1
~~~

* 指定并行度

~~~shell
# 默认值：-1
# 值类型：Integer
# 流批任务：流、批任务都支持
# 用处：可以用此参数设置 Flink SQL 中算子的并行度，这个参数的优先级 `高于` StreamExecutionEnvironment 中设置的并行度优先级，如果这个值设置为 -1，则代表没有设置，会默认使用 StreamExecutionEnvironment 设置的并行度
table.exec.resource.default-parallelism: -1
~~~

* sink数据处理

~~~shell
# 默认值：ERROR
# 值类型：Enum【ERROR, DROP】
# 流批任务：流、批任务都支持
# 用处：表上的 NOT NULL 列约束强制不能将 NULL 值插入表中。Flink 支持 `ERROR`（默认）和 `DROP` 配置。默认情况下，当 NULL 值写入 NOT NULL 列时，Flink 会产生运行时异常。用户可以将行为更改为 `DROP`，直接删除此类记录，而不会引发异常。
table.exec.sink.not-null-enforcer: ERROR
~~~

* CDC去重

~~~shell
# 默认值：false
# 值类型：Boolean
# 流批任务：流任务
# 用处：接入了 CDC 的数据源，上游 CDC 如果产生重复的数据，可以使用此参数在 Flink 数据源算子进行去重操作，去重会引入状态开销
table.exec.source.cdc-events-duplicate: false
~~~

* 配置空闲等待时间

~~~shell
# 默认值：0 ms
# 值类型：Duration
# 流批任务：流任务
# 用处：如果此参数设置为 60 s，当 Source 算子在 60 s 内未收到任何元素时，这个 Source 将被标记为临时空闲，此时下游任务就不依赖此 Source 的 Watermark 来推进整体的 Watermark 了。
# 默认值为 0 时，代表未启用检测源空闲。
table.exec.source.idle-timeout: 0 ms
~~~

* 配置状态有效期

~~~shell
# 默认值：0 ms
# 值类型：Duration
# 流批任务：流任务
# 用处：指定空闲状态（即未更新的状态）将保留多长时间。尤其是在 unbounded 场景中很有用。默认 0 ms 为不清除空闲状态
table.exec.state.ttl: 0 ms
~~~

### 优化器参数

* 两阶段提交

~~~shell
#  默认值：AUTO
#  值类型：String
#  流批任务：流、批任务都支持
#  用处：聚合阶段的策略。和 MapReduce 的 Combiner 功能类似，可以在数据 shuffle 前做一些提前的聚合，可以选择以下三种方式
#  TWO_PHASE：强制使用具有 localAggregate 和 globalAggregate 的两阶段聚合。请注意，如果聚合函数不支持优化为两个阶段，Flink 仍将使用单阶段聚合。
#  两阶段优化在计算 count，sum 时很有用，但是在计算 count distinct 时需要注意，key 的稀疏程度，如果 key 不稀疏，那么很可能两阶段优化的效果会适得其反
#  ONE_PHASE：强制使用只有 CompleteGlobalAggregate 的一个阶段聚合。
#  AUTO：聚合阶段没有特殊的执行器。选择 TWO_PHASE 或者 ONE_PHASE 取决于优化器的成本。
#  
#  注意！！！：此优化在窗口聚合中会自动生效，但是在 unbounded agg 中需要与 minibatch 参数相结合使用才会生效
table.optimizer.agg-phase-strategy: AUTO
~~~

* 分桶

~~~shell
#  默认值：false
#  值类型：Boolean
#  流批任务：流任务
#  用处：避免 group by 计算 count distinct\sum distinct 数据时的 group by 的 key 较少导致的数据倾斜，比如 group by 中一个 key 的 distinct 要去重 500w 数据，而另一个 key 只需要去重 3 个 key，那么就需要先需要按照 distinct 的 key 进行分桶。将此参数设置为 true 之后，下面的 table.optimizer.distinct-agg.split.bucket-num 可以用于决定分桶数是多少
#  后文会介绍具体的案例
table.optimizer.distinct-agg.split.enabled: false

#  默认值：1024
#  值类型：Integer
#  流批任务：流任务
#  用处：避免 group by 计算 count distinct 数据时的 group by 较少导致的数据倾斜。加了此参数之后，会先根据 group by key 结合 hash_code（distinct_key）进行分桶，然后再自动进行合桶。
#  后文会介绍具体的案例
table.optimizer.distinct-agg.split.bucket-num: 1024
~~~

* 查询计划重用

~~~shell
#  默认值：true
#  值类型：Boolean
#  流批任务：流任务
#  用处：如果设置为 true，Flink 优化器将会尝试找出重复的自计划并重用。默认为 true 不需要改动
table.optimizer.reuse-sub-plan-enabled: true
~~~

* Source端重用

~~~shell
#  默认值：true
#  值类型：Boolean
#  流批任务：流任务
#  用处：如果设置为 true，Flink 优化器会找出重复使用的 table source 并且重用。默认为 true 不需要改动
table.optimizer.reuse-source-enabled: true
~~~

* 谓词下推

~~~shell
#  默认值：true
#  值类型：Boolean
#  流批任务：流任务
#  用处：如果设置为 true，Flink 优化器将会做谓词下推到 FilterableTableSource 中，将一些过滤条件前置，提升性能。默认为 true 不需要改动
table.optimizer.source.predicate-pushdown-enabled: true
~~~

### 表参数

* 异步DML

~~~shell
#  默认值：false
#  值类型：Boolean
#  流批任务：流、批任务都支持
#  用处：DML SQL（即执行 insert into 操作）是异步执行还是同步执行。默认为异步（false），即可以同时提交多个 DML SQL 作业，如果设置为 true，则为同步，第二个 DML 将会等待第一个 DML 操作执行结束之后再执行
table.dml-sync: false
~~~

* 最长的方法名

~~~shell
#  默认值：64000
#  值类型：Integer
#  流批任务：流、批任务都支持
#  用处：Flink SQL 会通过生产 java 代码来执行具体的 SQL 逻辑，但是 jvm 限制了一个 java 方法的最大长度不能超过 64KB，但是某些场景下 Flink SQL 生产的 java 代码会超过 64KB，这时 jvm 就会直接报错。因此此参数可以用于限制生产的 java 代码的长度来避免超过 64KB，从而避免 jvm 报错。
table.generated-code.max-length: 64000
~~~

* 时区

~~~shell
#  默认值：default
#  值类型：String
#  流批任务：流、批任务都支持
#  用处：在使用天级别的窗口时，通常会遇到时区问题。举个例子，Flink 开一天的窗口，默认是按照 UTC 零时区进行划分，那么在北京时区划分出来的一天的窗口是第一天的早上 8:00 到第二天的早上 8:00，但是实际场景中想要的效果是第一天的早上 0:00 到第二天的早上 0:00 点。因此可以将此参数设置为 GMT+08:00 来解决这个问题。
table.local-time-zone: default
~~~

* 执行器

~~~shell
#  默认值：default
#  值类型：Enum【BLINK、OLD】
#  流批任务：流、批任务都支持
#  用处：Flink SQL planner，默认为 BLINK planner，也可以选择 old planner，但是推荐使用 BLINK planner
table.planner: BLINK
~~~

* SQL方言

~~~shell
#  默认值：default
#  值类型：String
#  流批任务：流、批任务都支持
#  用处：Flink 解析一个 SQL 的解析器，目前有 Flink SQL 默认的解析器和 Hive SQL 解析器，其区别在于两种解析器支持的语法会有不同，比如 Hive SQL 解析器支持 between and、rlike 语法，Flink SQL 不支持
table.sql-dialect: default
~~~

### 总结

上述参数中，有些参数已经默认做了优化。其余可以选择。常用的优化参数可以有：

~~~shell
微批处理
配置状态有效期
两阶段提交
分桶
~~~

### SQL优化

#### 微批处理

![1693048040833](assets/1693048040833.png)

SQL配置：

~~~shell
table.exec.mini-batch.enabled: true
table.exec.mini-batch.allow-latency: 5000 ms
table.exec.mini-batch.size: 1000
~~~

代码配置：

~~~shell
tEnv.getConfig().set("table.exec.mini-batch.enabled", "true"); //启用MiniBatch聚合
tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5 s"); //buffer最多5s的输入数据记录
tEnv.getConfig().set("table.exec.mini-batch.size", "5000"); //buffer最多的输入数据记录数目
~~~

#### 两阶段聚合

![1693048268905](assets/1693048268905.png)

SQL配置：

~~~shell
table.optimizer.agg-phase-strategy: TWO_PHASE
~~~

代码配置：

~~~shell
tEnv.getConfig().set("table.exec.mini-batch.enabled", "true"); //启用MiniBatch聚合
tEnv.getConfig().set("table.exec.mini-batch.allow-latency", "5 s"); //buffer最多5s的输入数据记录
tEnv.getConfig().set("table.exec.mini-batch.size", "5000"); //buffer最多的输入数据记录数目
tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE"); //打开两阶段聚合
~~~

#### 分桶

![1693048689678](assets/1693048689678.png)

SQL配置：

~~~shell
table.optimizer.distinct-agg.split.enabled: true
table.optimizer.distinct-agg.split.bucket-num: 1024
~~~

代码配置：

~~~shell
tEnv.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true")
tEnv.getConfig().set("table.optimizer.distinct-agg.split.bucket-num", "1024")
~~~

#### filter子句

用于case when场景下。

传统的写法：

~~~shell
SELECT
 day,
 COUNT(DISTINCT user_id) AS total_uv,
 COUNT(DISTINCT CASE WHEN flag IN ('android', 'iphone') THEN user_id ELSE NULL END) AS app_uv,
 COUNT(DISTINCT CASE WHEN flag IN ('wap', 'other') THEN user_id ELSE NULL END) AS web_uv
FROM T
GROUP BY day
~~~

filter子句写法：

~~~shell
SELECT
 day,
 COUNT(DISTINCT user_id) AS total_uv,
 COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('android', 'iphone')) AS app_uv,
 COUNT(DISTINCT user_id) FILTER (WHERE flag IN ('web', 'other')) AS web_uv
FROM T
GROUP BY day
~~~

filter的优化体现在指标结果的优化上。

如果是传统的写法，则每一个指标都会保存一个状态。这样会把结果保存为3个状态。如果想获取最终结果，则需要读取3个不同的状态。效率低。

如果是filter的写法，它可以把上面的3个状态合并为一个共享的状态。只需要读取这一个状态就可以获取最终结果了。提升读取效率。

## Flink项目

### 项目说明

~~~shell
#1.技术亮点
FlinkCDC（实时数据采集）
Hudi（数据湖）
Doris（实时OLAP数据库）
dlink（部署平台）
metabase（可视化）


#2.项目背景
在线教育平台——博学谷


#3.项目的环境
node1节点，内存推荐10G及以上
用户名：root
密码：123456
MySQL的用户名和密码同上。
~~~

### 教育行业背景

2020年疫情影响。

2021年中旬双减政策：专门针对教育行业的。

~~~shell
#1.禁止文化课课外补习。比如：语文、数学、英语等。
#2.提倡技能课的学习。舞蹈、音乐、架子鼓、书法、跆拳道等。
~~~

### 传统数据集成方案及分析

#### DataX

DataX 是阿里云 [DataWorks数据集成](https://www.aliyun.com/product/bigdata/ide) 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括
MySQL、Oracle、OceanBase、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS, databend
等各种异构数据源之间高效的数据同步功能。

快速入门链接：https://github.com/alibaba/DataX/blob/master/userGuid.md

DataX：可以永远各种异构数据源之间的 同步。它是一个同步工具。

#### Canal

![1693052642835](assets/1693052642835.png)

**canal [kə'næl]**，译意为水道/管道/沟渠，主要用途是基于 MySQL 数据库增量日志解析，提供增量数据订阅和消费

早期阿里巴巴因为杭州和美国双机房部署，存在跨机房同步的业务需求，实现方式主要是基于业务 trigger 获取增量变更。从 2010 年开始，业务逐步尝试数据库日志解析获取增量变更进行同步，由此衍生出了大量的数据库增量订阅和消费业务。

当前的 canal 支持源端 MySQL 版本包括 5.1.x , 5.5.x , 5.6.x , 5.7.x , 8.0.x。

![1693052601423](assets/1693052601423.png)

- MySQL master 将数据变更写入二进制日志( binary log, 其中记录叫做二进制日志事件binary log events，可以通过 show binlog events 进行查看)
- MySQL slave 将 master 的 binary log events 拷贝到它的**中继**日志(relay log)
- MySQL slave 重放 relay log 中事件，将数据变更反映它自己的数据

Canal和DataX可以配合使用，也可以分开，他们是分别独立的组件。

### 博学谷大数据平台

#### 早期的架构

![1693053062078](assets/1693053062078.png)

优点：架构简介，也能满足需求，Clickhouse支持大表，海量存储，单表性能强悍

缺点：压力全给到了Clickhouse、Clickhouse多表join性能不好

#### 现在的架构

必须掌握。

![1693053737884](assets/1693053737884.png)

ODS：源数据层（贴源层）

DWD：中间层（宽表层）

DWS：结果层（聚合层）

Flink在项目中，使用的是FlinkSQL，它的场景为流式管道。

数据流向如下：

![1693054125700](assets/1693054125700.png)

这个项目是一个湖仓一体架构。数据湖的数据会落入在数据仓库Hive中。

具体的技术组件后面会详细介绍。

### 技术选型

#### 为什么选FlinkCDC

FlinkCDC是一个用于实时数据采集组件，可以支持全量和增量数据采集。底层是Debezium来实现的。

FlinkCDC官网上的核心特点有三个：

* exactly-once（精准一次）
* DataStream API
* SQL

项目中使用的FlinkCDC是2.x的版本。没有使用早期的1.x的版本。

因为2.x的版本优点很明显：

* 无锁读取
* 并发读取
* 断点续传

上述这3个2.x的特点在1.x中都没有。所以不用1.x的版本。

#### 为什么选Hudi

Hudi是一个数据湖。市场上目前的开源的数据湖框架主要有三个：

* Delta Lake
* Apache Iceberg
* Apache Hudi

Hudi的特点主要有如下三个：

* 支持事务
* 行级别更新/删除
* 解锁新的查询姿势（增量查询）

#### 为什么选Doris

目前市场上实时的数据分析的数据库功能比较晚上组件有：Clickhouse、Doris。

* 高吞吐

* 低延迟

* 高性能

* 兼容MySQL协议

* 极简运维
* 多表Join性能优于Clickhouse

#### 为什么选FlinkSQL

流式计算框架中，能选的组件不多。Storm、StructuredStreaming、Flink。

* 高吞吐、低延迟、高性能

* 支持时间、窗口、水印等特性

* 支持容错、保存点

* 背压

### 框架软件版本

| **软件**  | **版本**  |
| --------- | --------- |
| Mysql     | 5.7       |
| Java      | 1.8.0_241 |
| Hadoop    | 3.3.0     |
| Zookeeper | 3.4.6     |
| Hive      | 3.1.2     |
| Flink     | 1.14.5    |
| Hudi      | 0.11.1    |
| Doris     | 1.1.0     |
| Dinky     | 0.6.6     |
| Flink CDC | 2.2.0     |

### 非功能描述

#### 平台版本

* CDH平台

Cloudera公司研发，截止在2021年1月31日，全球开源开源平台，开源的最后一个版本为6.3.x。

收费后它的名字换为CDP，版本从7.x开始。

* HDP平台

Hortonworks公司研发的，和CDH类似，也是全球的开源大数据平台。早些年被Cloudera公司收购了，因此HDP平台也归属于CDH了。

最后一个免费的HDP平台的版本为3.1.5。

* 华为：FI（FusionInsight），收费

* 腾讯：TBDS，收费
* 阿里：MaxCompute，收费
* 星环：TDH，收费，这个平台不亚于TBDS

* 自建平台

采用Apache原生的组件，公司内部自己组建平台。

优点：免费，可以尝鲜最新版。

缺点：公司自己处理版本与版本之间的兼容问题，依赖问题等

#### 服务器选型

物理机：公司自己自建机房，购买服务器。

云主机：使用阿里云、亚马逊云等云主机产品。

#### 集群规模

怎么说都对。

#### 人员配置

怎么解释都行。

#### 开发周期

怎么说都对。

### 博学谷大数据平台业务

略。后面详细解释。

### Flink整合Hive

#### 版本兼容

| **Hive大版本号** | **Hive小版本号**                                |
| ---------------- | ----------------------------------------------- |
| 1.0              | 1.0.0、1.0.1                                    |
| 1.1              | 1.1.0、1.1.1                                    |
| 1.2              | 1.2.0、1.2.1、1.2.2                             |
| 2.0              | 2.0.0、2.0.1                                    |
| 2.1              | 2.1.0、2.1.1                                    |
| 2.2              | 2.2.0                                           |
| 2.3              | 2.3.0、2.3.1、2.3.2、2.3.3、2.3.4、2.3.5、2.3.6 |
| 3.1              | 3.1.0、3.1.1、3.1.2                             |

说明：

~~~shell
Hive built-in functions are supported in 1.2.0 and later.
Column constraints, i.e. PRIMARY KEY and NOT NULL, are supported in 3.1.0 and later.
Altering table statistics is supported in 1.2.0 and later.
DATE column statistics are supported in 1.2.0 and later.
Writing to ORC tables is not supported in 2.0.x.

中文为：
Hive 内置函数在使用 Hive-1.2.0 及更高版本时支持。
列约束，也就是 PRIMARY KEY 和 NOT NULL，在使用 Hive-3.1.0 及更高版本时支持。
更改表的统计信息，在使用 Hive-1.2.0 及更高版本时支持。
DATE列统计信息，在使用 Hive-1.2.0 及更高版时支持。
使用 Hive-2.0.x 版本时不支持写入 ORC 表。
~~~

#### 整合方式

Flink可以有2种整合方式。

##### Using bundled hive jar

推荐使用，使用官网提供的bundled jar，把jar包方在`$FLINK_HOME/lib`目录下即可。

##### User defined dependencies

自己分别添加jar包。当第一种情况无法满足需求时才使用，不推荐。

#### SQL整合

~~~shell
CREATE CATALOG myhive WITH (
'type'='hive',
'hive-conf-dir'='/export/server/hive/conf',
'hive-version'='3.1.2',
'hadoop-conf-dir'='/export/server/hadoop/etc/hadoop/'
);
USE CATALOG myhive;
~~~

#### 演示

##### 启动服务

~~~shell
#1.HDFS
start-dfs.sh

#2.Hive
nohup hive --service metastore > /tmp/hive-metastore.log &

#3.Flink
start-cluster.sh

#4.进入sql-client
sql-client.sh -i conf/hive-catalog.sql
~~~

##### 演示

在Hive中创建库、表，在Flink中往Hive的表中插入数据，在Hive查询。

~~~shell
#1.在Hive中创建库
create database flink_demo;

#2.在Hive中切换库
use flink_demo;

#3.在Hive中创建表
CREATE TABLE IF NOT EXISTS `flink_demo.users`(
    `id` int, 
    `name` string
)ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

#4.在FlinkSQL中查看Hive创建的库
show databases;

#5.在FlinkSQL中切换库
use flink_demo;

#6.FlinkSQL查看Hive创建的表
show tables;

#7.在FlinkSQL中查看表数据
select * from users;

#8.在FlinkSQL中插入数据
insert into users values(1,'zhangsan');

#9.在FlinkSQL中查看表数据
select * from users;

#10.在Hive客户端中查看表数据
select * from users;
~~~

截图如下：

![1693058871359](assets/1693058871359.png)
