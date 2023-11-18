# Flink基础

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

英文：tumble

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
窗口大小为5秒，统计5秒之内的价格的总和。
~~~

##### 分析

~~~shell
//1.构建流式执行环境



//2.数据源Source



//3.数据处理Transformation
//3.1 把输入的数据转换为Tuple3<String, Integer, Long>
//3.2 给数据添加水印（watermark），这里指定为单调递增水印（设置延迟时间为0）
//3.3 对数据按照id进行分组/分流
//3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
//3.5 对窗口内的数据进行统计操作（sum）
//3.6 把Tuple3<String, Integer, Long>转换为Tuple2<String, Integer>



//4.数据输出Sink



//5.启动流式任务
~~~

##### 实现

~~~java
package day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: itcast
 * @date: 2023/8/15 19:45
 * @desc: 演示基于事件时间的滚动窗口，数据源来自于socket(id,price,ts)，类型为：String,Integer,Long。
 * 窗口大小为5秒，统计5秒之内的价格的总和。
 */
public class Demo01_TumbleWindow {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把输入的数据转换为Tuple3<String, Integer, Long>
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> mapDS = sourceDS.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) throws Exception {
                //value就是输入的一行数据：1001,10,1
                String[] lines = value.split(",");
                //把数据组织称Tuple3的形式返回：String id,Integer price,Long ts
                return Tuple3.of(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        mapDS.print("源数据");
        //3.2 给数据添加水印（watermark），这里指定为单调递增水印（设置延迟时间为0）
        /**
         * 这一步今晚仅做了解，不需要掌握，后面会单独讲
         * 这四个方法，是水印的四种策略。分别如下：
         * forMonotonousTimestamps：单调递增水印，效果类似于延迟时间为0
         * forBoundedOutOfOrderness:固定延迟水印，这种水印用的最多，后面再讲
         * forGenerator:自定义水印，一般不用
         * noWatermarks:不指定水印，一般不用
         *
         * withTimestampAssigner：给数据中哪一列分配水印
         * SerializableTimestampAssigner<Tuple3<String, Integer, Long>>：泛型就是输入的数据类型，这里就是Tuple3
         */
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                    /**
                     * extractTimestamp方法，指定数据中的事件时间列
                     * @param element 输入的数据
                     * @param recordTimestamp 事件时间
                     * @return 数据中的事件时间
                     */
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                        //这里指定事件时间是哪个列就可以了，这里指定为ts列即可
                        return element.f2 * 1000L;//需要乘以1000，转换为毫秒值
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = watermarkDS.keyBy(value -> value.f0)//指定分组的数据类型，这里用的是id分组
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 对窗口内的数据进行统计操作（sum）,String id,Integer price,Long ts
                .sum(1)//1表示用数据中第二列来进行求和
                //3.6 把Tuple3<String, Integer, Long>转换为Tuple2<String, Integer>
                .map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                });


        //4.数据输出Sink
        result.print("聚合后的结果：");


        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692102410044](assets/1692102410044.png)

#### SQL案例 -TVF写法（了解）

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
（1）把滚动窗口的定义放在from子句后面，而不是放在group by后面了
（2）定义的语法如下，需要指定3个参数：
from table(窗口类型(table 表名,descriptor(事件时间列), 窗口大小))
比如：
from table(tumble(table source_table,descriptor(row_time), interval '5' second))


#3.数据处理SQL
SELECT 
    user_id,
    UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start,
    UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end,
    sum(price) as sum_price
FROM TABLE(TUMBLE(
        TABLE source_table
        , DESCRIPTOR(row_time)
        , INTERVAL '5' SECOND))
GROUP BY window_start, 
      window_end,
      user_id;
~~~

截图如下：

![1692103029928](assets/1692103029928.png)

### 滑动窗口

#### 概述

![1692104630190](assets/1692104630190.png)

滑动窗口：窗口大小  != 滚动/滑动距离

滑动窗口，英文有两种说法：Hop（FlinkSQL）、Slide（API）

~~~shell
滑动窗口：窗口大小 != 滚动/滑动距离

（1）滑动距离 < 窗口大小，这种情况下，会产生数据重复计算。比如：每隔半小时，统计最近1天的数据。咋们要重点讨论。
（2）滑动距离 = 窗口带下，这种情况下，就是我们滚动窗口了。这里就不讨论了。
（3）滑动距离 > 窗口大小，这种情况下，会产生数据丢失。所以，这里我们就不讨论了。比如：每隔1个月，统计最近1天的数据。
~~~

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



#2.定义
（1）把滑动窗口的整个写法放置在group by子句后面
（2）hop(事件时间列, 滑动间隔, 窗口大小)
比如：hop(row_time, interval '2' second, interval '5' second)


#3.数据处理SQL
SELECT user_id,
UNIX_TIMESTAMP(CAST(hop_start(row_time, interval '2' SECOND, interval '5' SECOND) AS STRING)) * 1000 as window_start,
UNIX_TIMESTAMP(CAST(hop_end(row_time, interval '2' SECOND, interval '5' SECOND) AS STRING)) * 1000 as window_end, 
    sum(price) as sum_price
FROM source_table
GROUP BY user_id
    , hop(row_time, interval '2' SECOND, interval '5' SECOND);
~~~

分析如下：

~~~shell
前提条件：窗口的大小为5秒钟，滑动距离为2秒钟
#1.窗口的起始
窗口的起始时间 = 第一条数据的事件时间 - (第一条数据的事件时间 % 窗口大小)
            = 1 - (1 % 5)
			= 1 - 1
			= 0

又由于窗口的大小为5秒钟，滑动距离为2秒，因此，窗口的排布如下：

#2. 窗口的排布
滑动窗口，因此窗口可以往前滑动，也可以往后滑动
...[-6，-1)、 [-4,1)、[-2,3)，[0,5)，[2,7)，[4,9) ，[6,11) ...

因为第一条数据的事件时间为1，
也就是说，事件时间为1的这条数据，应该落入到它能落入的所有窗口之内。这样数据才能重复计算！
      ... [-6，-1)、[-4,1)、[-2,3)，[0,5)，[2,7)，[4,9) ，[6,11) ...
窗口：  0    1        2        3       4     5      6      7     8
请问哪些窗口还在？哪些窗口不在？
0,1,2这3个窗口不在了。
其他窗口，都会陆陆续续开启。


~~~

截图如下：

![1692105415602](assets/1692105415602.png)

#### SQL案例 - 扩展

其他条件都不变，唯独输入的数据有变化。

~~~shell
1001,10,0
~~~

分析：

~~~shell
前提条件：窗口的大小为5秒钟，滑动距离为2秒钟
#1.窗口的起始
窗口的起始时间 = 第一条数据的事件时间 - (第一条数据的事件时间 % 窗口大小)
            = 0 - (0 % 5)
			= 0 - 0
			= 0

又由于窗口的大小为5秒钟，滑动距离为2秒，因此，窗口的排布如下：

#2. 窗口的排布
滑动窗口，因此窗口可以往前滑动，也可以往后滑动
...[-6，-1)、 [-4,1)、[-2,3)，[0,5)，[2,7)，[4,9) ，[6,11) ...

因为第一条数据的事件时间为0，
也就是说，事件时间为0的这条数据，应该落入到它能落入的所有窗口之内。这样数据才能重复计算！
      ... [-6，-1)、[-4,1)、[-2,3)，[0,5)，[2,7)，[4,9) ，[6,11) ...
窗口：  0    1        2        3       4     5      6      7     8
请问哪些窗口还在？哪些窗口不在？
0,1这2个窗口不在了。
其他窗口，都会陆陆续续开启。

~~~

截图如下：

![1692107408982](assets/1692107408982.png)

#### SQL案例 - TVF写法（了解）

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



#2.定义
（1）把滑动窗口的定义放在from子句后面，而不是放在group by后面了
（2）定义的语法如下，需要指定4个参数：窗口大小必须是滑动距离的整数倍。
from table(窗口类型(table 表名,descriptor(事件时间列), 滑动距离, 窗口大小))

比如：
from table(hop(table source_table,descriptor(row_time), interval '2' second,interval '5' second))



#3.数据处理的SQL
SELECT 
    user_id,
UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 as window_start,  
UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, 
    sum(price) as sum_price
FROM TABLE(HOP(
        TABLE source_table
        , DESCRIPTOR(row_time)
        , interval '2' SECOND, interval '6' SECOND))
GROUP BY window_start, 
      window_end,
      user_id;
~~~

截图如下：

![1692107926350](assets/1692107926350.png)

#### DataStream案例

##### 需求

~~~shell
演示基于事件时间的滑动窗口，数据源来自于socket(id,price,ts)，类型为：String,Integer,Long。
窗口大小为5秒，统计5秒之内的价格的总和。
~~~

##### 分析

~~~shell
//1.构建流式执行环境



//2.数据源Source



//3.数据处理Transformation
//3.1 把输入的数据转换为Tuple3<String, Integer, Long>
//3.2 给数据添加水印（watermark），这里指定为单调递增水印（设置延迟时间为0）
//3.3 对数据按照id进行分组/分流
//3.4 划分窗口，这里指定为滑动窗口，窗口大小为5秒钟，滑动距离2秒钟
//3.5 对窗口内的数据进行统计操作（sum）
//3.6 把Tuple3<String, Integer, Long>转换为Tuple2<String, Integer>



//4.数据输出Sink



//5.启动流式任务
~~~

##### 实现

~~~java
package day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: itcast
 * @date: 2023/8/17 19:37
 * @desc: 需求：演示基于事件时间的滑动窗口，数据源来自于socket(id,price,ts)，类型为：String,Integer,Long。
 * 窗口大小为5秒，统计5秒之内的价格的总和。
 */
public class Demo01_SlideWindow {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);


        //3.数据处理Transformation
        //3.1 把输入的数据转换为Tuple3<String, Integer, Long>
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> mapDS = sourceDS.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) throws Exception {
                String[] lines = value.split(",");
                return Tuple3.of(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        mapDS.print("源数据");

        //3.2 给数据添加水印（watermark），这里指定为单调递增水印（设置延迟时间为0）
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                        return element.f2 * 1000L;
                    }
                }));

        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = watermarkDS.keyBy(value -> value.f0)
                //3.4 划分窗口，这里指定为滑动窗口，窗口大小为5秒钟，滑动距离2秒钟
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                //3.5 对窗口内的数据进行统计操作（sum）
                .sum(1)
                //3.6 把Tuple3<String, Integer, Long>转换为Tuple2<String, Integer>
                .map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                });


        //4.数据输出Sink
        result.print("聚合后的结果");

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692273226257](assets/1692273226257.png)

### 会话窗口

#### 概述

会话，英文称之为session，在Flink中，响铃的两条数据，它的事件时间如果超过窗口的会话大小，则这两条数据会被划分到不同的会话中。

相反，则会被划分到相同的会话中。

会话窗口有固定的会话间隔，如果相邻的两条数据的事件时间**超过**了会话间隔，则会被划分到不同的窗口中去。反之，则在同一个窗口。

![1692273726082](assets/1692273726082.png)

#### SQL入门案例

~~~shell
#创建表
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


#2.会话窗口定义
(1)整个窗口放置在group by子句后面
(2)session(事件时间列, 会话间隔)
比如：
session(row_time, interval '5' second)


#3.数据处理SQL
SELECT 
    user_id,
UNIX_TIMESTAMP(CAST(session_start(row_time, interval '5' SECOND) AS STRING)) * 1000 as window_start,
UNIX_TIMESTAMP(CAST(session_end(row_time, interval '5' SECOND) AS STRING)) * 1000 as window_end, 
    sum(price) as sum_price
FROM source_table
GROUP BY user_id
      , session(row_time, interval '5' SECOND);
~~~

截图如下：

![1692274747110](assets/1692274747110.png)

#### DataStream案例

##### 需求

~~~shell
演示基于事件时间的会话窗口，数据源来自于socket(id,price,ts)，类型为：String,Integer,Long。
窗口的会话间隔为5秒，统会话窗口之内的水位信息总和。
~~~

##### 分析

~~~shell
//1.构建流式执行环境



//2.数据源Source



//3.数据处理Transformation
//3.1 把输入的数据转换为Tuple3<String, Integer, Long>
//3.2 给数据添加水印（watermark），这里指定为单调递增水印（设置延迟时间为0）
//3.3 对数据按照id进行分组/分流
//3.4 划分窗口，这里指定为会话窗口，间隔为5秒钟
//3.5 对窗口内的数据进行统计操作（sum）
//3.6 把Tuple3<String, Integer, Long>转换为Tuple2<String, Integer>



//4.数据输出Sink



//5.启动流式任务
~~~

##### 实现

~~~java
package day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: itcast
 * @date: 2023/8/17 20:26
 * @desc: 需求：演示基于事件时间的会话窗口，数据源来自于socket(id,price,ts)，类型为：String,Integer,Long。
 * 窗口的会话间隔为5秒，统会话窗口之内的水位信息总和。
 */
public class Demo02_SessionWindow {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);


        //3.数据处理Transformation
        //3.1 把输入的数据转换为Tuple3<String, Integer, Long>
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> mapDS = sourceDS.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String value) throws Exception {
                String[] lines = value.split(",");
                return Tuple3.of(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });

        //3.2 给数据添加水印（watermark），这里指定为单调递增水印（设置延迟时间为0）
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                        return element.f2 * 1000L;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = watermarkDS.keyBy(value -> value.f0)
                //3.4 划分窗口，这里指定为会话窗口，间隔为5秒钟,withGap:指定窗口的会话间隔
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                //3.5 对窗口内的数据进行统计操作（sum）
                .sum(1)
                //3.6 把Tuple3<String, Integer, Long>转换为Tuple2<String, Integer>
                .map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
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

![1692275810295](assets/1692275810295.png)

==备注：会话窗口，没有TVF方案的写法。==

### 渐进式窗口（了解）

#### 概述

cumulate，渐进式，在一定的时间范围内，结果呈现线性递增趋势。

比如：每天的营收额，每隔一小时统计一次。

#### SQL入门案例

~~~shell
#1.创建表
CREATE TABLE source_table (
    -- 用户 id
    user_id BIGINT,
    -- 用户
    money BIGINT,
    -- 事件时间戳
    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
    -- watermark 设置
    WATERMARK FOR row_time AS row_time - INTERVAL '0' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '100000',
  'fields.money.min' = '1',
  'fields.money.max' = '100000'
);


#2.定义
这个渐进式窗口，没有API，也没有传统的SQL案例，只有TVF案例。
(1)整个渐进式窗口放置在from子句后面
(2)from table(窗口类型(table 表名称,descriptor(事件时间列), 间隔，大小))
比如：
from table(cumulate(table source,descriptor(rt), interval '5' second，interval '30' second))
每隔5秒，统计最近30秒内的数据。


#3.数据处理SQL
SELECT 
FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(window_start AS STRING)))  as window_start,
    FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(window_end AS STRING))) as window_end, 
    sum(money) as sum_money,
    count(distinct user_id) as count_distinct_id
FROM TABLE(CUMULATE(
       TABLE source_table
       , DESCRIPTOR(row_time)
       , INTERVAL '5' SECOND
       , INTERVAL '30' SECOND))
GROUP BY
    window_start, 
    window_end;
~~~

截图如下：

![1692277854032](assets/1692277854032.png)

### 聚合窗口（了解）

#### 概述

Flink的聚合窗口，可以类似于Hive中的聚合一样，可以统计窗口中的数据。

主要分为两种类型的聚合窗口：

* 根据时间聚合
* 根据行数聚合

#### 时间聚合

~~~shell
#1.创建表
CREATE TABLE source_table (
    order_id BIGINT,
    product BIGINT,
    amount BIGINT,
    order_time as cast(CURRENT_TIMESTAMP as TIMESTAMP(3)),
    WATERMARK FOR order_time AS order_time - INTERVAL '0' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.order_id.min' = '1',
  'fields.order_id.max' = '2',
  'fields.amount.min' = '1',
  'fields.amount.max' = '10',
  'fields.product.min' = '1',
  'fields.product.max' = '2'
);


#2.定义
xxx(聚合函数) over(range between 起始时间 and 当前时间)
比如：
sum(money) over(range between interval '1' hour preceding and current row)
起始时间：interval '1' hour preceding
结束时间：current row

#3处理数据的SQL
SELECT product, order_time, amount,
  SUM(amount) OVER (
    PARTITION BY product
    ORDER BY order_time
    -- 标识统计范围是一个 product 的最近 30秒的数据
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
  ) AS one_hour_prod_amount_sum
FROM source_table;
~~~

截图如下：

![1692278440478](assets/1692278440478.png)

#### 根据行号聚合

~~~shell
#1.建表
和前面一样


#2.定义
xxx(聚合函数) over(row between 起始行数 and 当前行数)
比如：
sum(money) over(row between 100 preceding and current row)
起始时间：100 preceding
结束时间：current row


#3.数据处理SQL
SELECT product, order_time, amount,
  SUM(amount) OVER (
    PARTITION BY product
    ORDER BY order_time
    -- 标识统计范围是一个 product 的最近 100 行数据
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
  ) AS one_hour_prod_amount_sum
FROM source_table;
~~~

截图如下：

![1692278696281](assets/1692278696281.png)

## Flink的watermark

* 为什么学watermark
* watermark的概述
* SQL案例（延迟时间为0）
* SQL案例（延迟时间不为0）
* watermark下的窗口触发策略
* SQL案例（watermark不为0）
* DataStream案例（单调递增水印）
* DataStream案例（固定延迟水印）
* DataStream案例（allowLateness）
* DataStream案例（SideOutput）
* DataStream案例（多并行度）
* watermark对齐问题
* DataStream案例（withIdleness）
* 面试题：如何保证迟到数据不丢失

### 为什么要学watermark

**场景**：如果你开车进入了地下车库，进入车库之前，手机有信号，进去之后，手机可能没信号。

等停完车后，又有信号了。

火车进隧道、开车的朋友，当车辆驶入隧道内时，甚至进入电梯，信号会突然丢失等都属于类似的情况。

这种场景，咋们能避免嘛？

不能。

也就是说，当出现如上情况时，Flink作为一款流式处理框架，它对数据的时效性非常敏感，怎么办呢？

**分析**：

请问，数据不要行不行？也就是说，数据能不能丢？

坚决不行。

无论什么情况，数据绝对不能丢。

那怎么办呢？

问题：数据因为信号、网络等因素造成数据有一定的延迟现象。

底线：数据不能丢弃。一定要。

怎么处理？

**举例**：

公司周日安排团建，去xxx地方游玩，晚上大餐。周日早上8:00到公司楼下集合。公司安排车辆统一接送。

比如说，有个小伙伴，它路上堵车了，会晚到1分钟。

请问这种情况，我们是等还是不等？

等。

比如说，有个小伙伴，它临时家里有事，无法8点准时到达，需要2个小时的时间处理，10点后才能去。

请问这种情况，我们是等还是不等？

不等。

生活中，我们可以这么处理，Flink程序中，同样如此。

**总结**：

Flink对于正常迟到的数据，我们可以等待一会儿。等待的时间可以用户自定义设置。

Flink对于一些异常迟到的数据，我们可以有其他的处理方案。

我们把对于正常迟到数据，等会儿处理的情况，称之为延迟处理。

这种机制就是watermark机制。

这也是我们学watermark的原因。

### watermark概述

watermark，俗称水印，或者水位线，它设计的目的**就是为了处理一定时间内的迟到数据**。

一定时间内：也就是在固定的时间内，可以处理，如果超过了固定时间了，这种情况无法处理。

这个固定时间，就是数据的正常迟到时间。也就是程序等待的时间，也就是延迟时间。

### SQL案例（延迟时间为0）

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



#2.watermark的定义
WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
watermark for 事件时间列 as watermark的策略表达式
watermark的策略表达式：就是延迟时间，也就是Flink程序延迟多久来触发计算
watermark for row_time as row_time - interval '0' second



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

![1692280778737](assets/1692280778737.png)



