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













