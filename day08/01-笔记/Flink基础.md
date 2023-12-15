## Flink的watermark

- 为什么学watermark
- watermark的概述
- SQL案例（延迟时间为0）
- SQL案例（延迟时间不为0）
- watermark下的窗口触发策略
- DataStream案例（单调递增水印）
- DataStream案例（固定延迟水印）
- DataStream案例（allowLateness）
- DataStream案例（SideOutput）
- DataStream案例（多并行度）
- watermark对齐问题
- DataStream案例（withIdleness）
- 面试题：如何保证迟到数据不丢失
- Checkpoint机制

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

```shell
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
```

截图如下：

![1692280778737](assets/1692280778737.png)

延迟正常迟到数据的情况：

![1692424389264](assets/1692424389264.png)

结论：延迟时间为0的情况下，正常迟到的数据会丢失。

### SQL案例（延迟时间不为0）

~~~shell
#1.创建表
CREATE TABLE source_table ( 
 user_id STRING, 
 price BIGINT,
 `timestamp` bigint,
 row_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`)),
 watermark for row_time as row_time - interval '2' second
) WITH (
  'connector' = 'socket',
  'hostname' = 'node1', 
  'port' = '9999',
  'format' = 'csv'
);


#2.设置水印的延迟时间为2秒钟
watermark for row_time as watermark策略表达式
watermark策略表达式：row_time - interval '2' second，也就是设置了2秒钟的延迟时间。


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

![1692424992809](assets/1692424992809.png)

延迟晚于watermark定义的延迟时间后到达的数据情况：

![1692425399464](assets/1692425399464.png)

结论：由于给了2秒钟的延迟时间，因此正常迟到（2秒钟内）的数据能够被正常处理。

但是如果数据的到达时间超过了watermark设置的延迟时间，则数据无法被正常处理，会丢失。

### 带有watermark下的窗口触发策略

==**窗口一旦带有watermark后，那么窗口的触发计算，就由watermark的时间来决定。**==

watermark的计算公式如下：

~~~shell
watermark的时间 = 事件时间 - 设置的延迟时间
~~~

#### 设置的延迟时间为0

如果延迟时间设置为0，则watermark的时间计算推导如下：

~~~shell
#计算推导
watermark的时间 = 事件时间 - 设置的延迟时间
			   = 事件时间 - 0
			   = 事件时间

#结论
watermark的时间 = 事件时间

也就是说，如果设置的延迟时间为0，则watermark的时间和事件时间是`相等的`。
因此，前面演示过程中，数据的事件时间为5时，会触发窗口计算。
~~~

#### 设置的延迟时间不为0

大于0，小于0？

如果是负数（小于0），则窗口内的数据会提前触发计算。这种情况现实中压根就不允许。

如果延迟时间设置不为0（设置为2），则watermark的时间计算推导如下：

~~~shell
#计算推导
watermark的时间 = 事件时间 - 设置的延迟时间
			   = 事件时间 - 2


#结论
watermark的时间 = 事件时间 - 2


#计算
当事件时间为1，则watermark的时间为：
watermark的时间 = 事件时间 - 2 = 1 - 2 = -1，没有达到窗口的触发计算时间5，因此不会触发窗口的计算
当事件时间为2，则watermark的时间为：
watermark的时间 = 事件时间 - 2 = 2 - 2 = 0，没有达到窗口的触发计算时间5，因此不会触发窗口的计算
当事件时间为5，则watermark的时间为：
watermark的时间 = 事件时间 - 2 = 5 - 2 = 3，没有达到窗口的触发计算时间5，因此不会触发窗口的计算
当事件时间为6，则watermark的时间为：
watermark的时间 = 事件时间 - 2 = 6 - 2 = 4，没有达到窗口的触发计算时间5，因此不会触发窗口的计算

**特例**
当事件时间为3，则watermark的时间为：事件时间会取最大值6，所以是6-2，而不是3-2
watermark的时间 = 事件时间 - 2 = 6 - 2 = 4，没有达到窗口的触发计算时间5，因此不会触发窗口的计算

当事件时间为7，则watermark的时间为：
watermark的时间 = 事件时间 - 2 = 7 - 2 = 5，已经达到窗口的触发计算时间5，因此会触发窗口的计算


#结论
watermark的时间，不会减少，只会往上递增。
~~~

总结：窗口中一旦带有watermark，则窗口的触发计算就由watermark的时间来决定。

### DataStream案例（单调递增水印）

#### 需求

~~~shell
从socket获取数据，转换成水位传感器类，基于事件时间，每5秒生成一个滚动窗口，来计算传感器水位信息
这里是水位传感类的类：
定义类 WaterSensor  String id; Integer vc; Long ts; 
~~~

#### 分析

~~~shell
//1.构建流式执行环境


//2.数据源Source


//3.数据处理Transformation
//3.1 把数据源转换为水位传感器的类WaterSensor
//3.2 给数据添加水印，这里指定为单调递增水印（现象和延迟时间设置为0是一样的）
//3.3 对数据按照id进行分组/分流
//3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
//3.5 调用偏底层的API（process）

//4.数据Sink


//5.启动流式任务

~~~

#### 实现

~~~java
package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/19 15:05
 * @desc: 从socket获取数据，转换成水位传感器类，基于事件时间，每5秒生成一个滚动窗口，来计算传感器水位信息
 * 这里是水位传感类的类：
 * 定义类 WaterSensor  String id; Integer vc; Long ts;
 */
public class Demo01_MonotonousTimestamp {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor> mapDS = sourceDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为单调递增水印（现象和延迟时间设置为0是一样的）
        /**
         * forMonotonousTimestamps：单调递增水印，也就是没延迟，和SQL中延迟时间设置为0效果一样，用的很少
         * forBoundedOutOfOrderness：固定延迟水印，就类似于设置了一定的延迟时间，用的最多，掌握
         * forGenerator：自定义水印，几乎不用
         * noWatermarks：没有水印，几乎不用
         */
        SingleOutputStreamOperator<WaterSensor> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;//使用数据中的ts属性当做事件时间的列
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window( .of(Time.seconds(5)))
                //3.5 调用偏底层的API（process）
                /**
                 * ProcessWindowFunction对象需要接收4个参数：
                 * 参数一：The type of the input value.输入数据类型，这里就是WaterSensor
                 * 参数二：The type of the output value.输出的数据类型，这个可以自定义，比如String类型
                 * 参数三：The type of the key.key的数据类型，key就是分组/分流，这里分组的类型就是String类型
                 * 参数四：The type of Window that this window function can be applied on.
                 *         窗口的类型，这里使用时间窗口
                 */
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * 这是process方法，它是一个底层的方法，比较灵活，在其他算子无法满足条件时，可以调用这个方法来计算
                     * @param key 分组/分流的数据
                     * @param context 上下文对象，可以获取一些其他的信息
                     * @param elements 表示窗口内的数据（元素）
                     * @param out 输出对象，也就是process处理完后，要使用out对象收集返回回去
                     * @throws Exception 异常信息
                     */
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        out.collect("分组的key为：" + key +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n当前的窗口为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark的值为：" + context.currentWatermark());
                    }
                });

        //4.数据Sink
        result.print();

        //5.启动流式任务
        env.execute();

    }
}

/**
 * 自定义的水位传感器的类
 */
class WaterSensor {
    //水位id
    private String id;
    //水位的值
    private Integer vc;
    //时间戳
    private Long ts;

    /**
     * 空参构造器
     */
    public WaterSensor() {
    }

    /**
     * 全参构造器
     * @param id id标识
     * @param vc 水位的值
     * @param ts 时间戳
     */
    public WaterSensor(String id, Integer vc, Long ts) {
        this.id = id;
        this.vc = vc;
        this.ts = ts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", vc=" + vc +
                ", ts=" + ts +
                '}';
    }
}
~~~

截图如下：

![1692431020068](assets/1692431020068.png)

#### 实现 - 注解版

~~~java
package day08;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/19 15:45
 * @desc: 单调递增水印，注解版
 */
public class Demo02_MonotonousTimestamp_02 {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor02> mapDS = sourceDS.map(new MapFunction<String, WaterSensor02>() {
            @Override
            public WaterSensor02 map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor02(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为单调递增水印（现象和延迟时间设置为0是一样的）
        SingleOutputStreamOperator<WaterSensor02> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor02>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor02>() {
                    @Override
                    public long extractTimestamp(WaterSensor02 element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 调用偏底层的API（process）
                .process(new ProcessWindowFunction<WaterSensor02, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor02> elements, Collector<String> out) throws Exception {
                        out.collect("分组的key为：" + s +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n窗口的大小为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark时间为：" + context.currentWatermark());
                    }
                });

        //4.数据Sink
        result.print();

        //5.启动流式任务
        env.execute();
    }

}

/**
 * 自定义的水位传感器的类
 * @Data: 可以构建getter、setter、toString等方法
 * @NoArgsConstructor: 无参构造器（空参构造器）
 * @AllArgsConstructor 全参构造器
 *
 * 补充说明：
 * lombok插件如果idea没有安装，则第一次使用需要安装。安装完后，重启就可以使用了。
 * pom中已经给了lombok的依赖了，不需要额外引入了。如果是公司中要用，则要引入lombok依赖。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
class WaterSensor02 {
    //id标识
    private String id;
    //vc的值
    private Integer vc;
    //时间戳
    private Long ts;
}


~~~

截图如下：

![1692432218821](assets/1692432218821.png)

### DataStream案例（固定延迟水印）

#### 需求

~~~shell
从socket获取数据，转换成水位传感器类，基于事件时间，每5秒生成一个滚动窗口，来计算传感器水位信息
这里是水位传感类的类：
定义类 WaterSensor  String id; Integer vc; Long ts; 
设置水印策略为固定延迟水印，延迟时间为2秒钟。
~~~

#### 分析

~~~shell
//1.构建流式执行环境


//2.数据源Source


//3.数据处理Transformation
//3.1 把数据源转换为水位传感器的类WaterSensor
//3.2 给数据添加水印，这里指定为固定延迟水印（现象和延迟时间设置为2秒钟是一样的）
//3.3 对数据按照id进行分组/分流
//3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
//3.5 调用偏底层的API（process）

//4.数据Sink


//5.启动流式任务

~~~

#### 实现

~~~java
package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: itcast
 * @date: 2023/8/19 16:27
 * @desc: 从socket获取数据，转换成水位传感器类，基于事件时间，每5秒生成一个滚动窗口，来计算传感器水位信息
 * 这里是水位传感类的类：
 * 定义类 WaterSensor  String id; Integer vc; Long ts;
 * 设置水印策略为固定延迟水印，延迟时间为2秒钟。
 */
public class Demo03_BoundedOutofOrderness {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor03> mapDS = sourceDS.map(new MapFunction<String, WaterSensor03>() {
            @Override
            public WaterSensor03 map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor03(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为固定延迟水印（现象和延迟时间设置为2秒钟是一样的）
        /**
         * forBoundedOutOfOrderness()：固定延迟时间，里面要跟一个参数
         * Duration.ofSeconds(2)：设置延迟时间，这里指定为2秒钟
         */
        SingleOutputStreamOperator<WaterSensor03> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor03>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor03>() {
                    @Override
                    public long extractTimestamp(WaterSensor03 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 调用偏底层的API（process）
                .process(new ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor03> elements, Collector<String> out) throws Exception {
                        out.collect("窗口内的key为：" + s +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n窗口大小为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark时间为：" + context.currentWatermark());
                    }
                });

        //4.数据Sink
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692434737197](assets/1692434737197.png)

结论：watermark能够处理正常迟到的数据，对于超过watermark设置的延迟时间到达的数据没办法处理，数据会丢失。

很显然，企业中不会运行有数据丢失。

### DataStream案例（allowLateness）

~~~java
package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: itcast
 * @date: 2023/8/19 16:27
 * @desc: 演示AllowLateness案例
 * AllowLateness：允许窗口销毁的延迟时间，默认情况下，窗口的销毁和窗口的触发计算时间是一致的。
 * 也就是说，窗口什么时候触发计算，就在什么时候销毁。
 * 如果添加上allowLateness，它可以在窗口触发计算后，再延迟allowLateness设置的时间后再销毁。
 * 这种现象在生活中也经常出现。
 */
public class Demo04_AllowLateness {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor03> mapDS = sourceDS.map(new MapFunction<String, WaterSensor03>() {
            @Override
            public WaterSensor03 map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor03(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为固定延迟水印（现象和延迟时间设置为2秒钟是一样的）
        /**
         * forBoundedOutOfOrderness()：固定延迟时间，里面要跟一个参数
         * Duration.ofSeconds(2)：设置延迟时间，这里指定为2秒钟
         */
        SingleOutputStreamOperator<WaterSensor03> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor03>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor03>() {
                    @Override
                    public long extractTimestamp(WaterSensor03 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 设置AllowLateness的延迟时间，这个时间就是延迟时间，它可以延迟窗口的销毁时间。比如设置为1秒钟
                .allowedLateness(Time.seconds(1))//设置allowLateness的延迟时间为1，运行窗口延迟1秒钟之后销毁
                //3.6 调用偏底层的API（process）
                .process(new ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor03> elements, Collector<String> out) throws Exception {
                        out.collect("窗口内的key为：" + s +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n窗口大小为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark时间为：" + context.currentWatermark());
                    }
                });

        //4.数据Sink
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692436447007](assets/1692436447007.png)

结论：在设置了allowLateness后，allowLateness可以延迟窗口的销毁时间。

默认情况下，窗口的销毁时间和窗口触发计算的时间是一致的。什么时候触发计算，就什么时候销毁。

如果设置了allowLateness，可以延长销毁的时间，也计算触发窗口计算后，暂时不销毁窗口，仍然允许数据进来。

但是，如果数据超过了allowLateness设置的销毁时间后到达，则数据仍然会丢失。

问：企业中，数据不允许丢失，那怎么办呢？

### DataStream案例（SideOutput）

SideOutput，Flink中的侧输出机制，侧道输出机制，侧输出流机制，旁路输出。

它可以捕获严重迟到的数据，不会让数据丢失。

~~~java
package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author: itcast
 * @date: 2023/8/19 19:13
 * @desc: 演示侧输出流机制，来捕获严重迟到的数据。
 */
public class Demo05_SideOutput {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor03> mapDS = sourceDS.map(new MapFunction<String, WaterSensor03>() {
            @Override
            public WaterSensor03 map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor03(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为固定递增水印，设置固定延迟时间为2秒钟
        SingleOutputStreamOperator<WaterSensor03> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor03>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor03>() {
                    @Override
                    public long extractTimestamp(WaterSensor03 element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        //创建OutputTag对象,这种方式会报错。
        OutputTag<WaterSensor03> lateData = new OutputTag<>("lateData", Types.GENERIC(WaterSensor03.class));
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 设置AllowLateness为1秒钟，把窗口的销毁时间再延迟1秒钟
                .allowedLateness(Time.seconds(1))
                //3.6 设置侧输出流，用来捕获严重迟到的数据
                .sideOutputLateData(lateData)
                //3.7 调用偏底层的API（process）
                .process(new ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>.Context context, Iterable<WaterSensor03> elements, Collector<String> out) throws Exception {
                        out.collect("窗口的key为：" + s +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n窗口大小为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark时间为：" + context.currentWatermark());
                    }
                });

        //4.数据Sink
        //4.1 这里输出的是正常处理后的数据结果
        result.print("正常数据");
        //4.2 输出严重迟到的数据
        result.getSideOutput(lateData).printToErr("异常迟到的数据");

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692445316654](assets/1692445316654.png)

结论：SideOutput机制，可以捕获严重迟到的数据，保证迟到数据不丢失。

### DataStream案例（多并行度）

~~~java
package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: itcast
 * @date: 2023/8/19 19:43
 * @desc: 演示多并行度下的水印
 */
public class Demo06_MultiParallelism {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为2
        env.setParallelism(2);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor03> mapDS = sourceDS.map(new MapFunction<String, WaterSensor03>() {
            @Override
            public WaterSensor03 map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor03(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为固定延迟水印，延迟2秒钟
        SingleOutputStreamOperator<WaterSensor03> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor03>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor03>() {
                    @Override
                    public long extractTimestamp(WaterSensor03 element, long recordTimestamp) {
                        System.out.println("线程号为：" + Thread.currentThread().getId() + "消费了事件时间为：" + element.getTs() + "的数据。");
                        return element.getTs() * 1000;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 调用偏底层的API（process）
                .process(new ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>.Context context, Iterable<WaterSensor03> elements, Collector<String> out) throws Exception {
                        out.collect("窗口的key为：" + s +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n窗口大小为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark时间为：" + context.currentWatermark());
                    }
                });
        //4.数据Sink
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692446506685](assets/1692446506685.png)

结论：当有多个并行度在消费数据时，水印时间以最小的并行度的watermark的时间为准。只要有一个并行度的watermark时间没有达到

窗口的临界条件，则窗口内的数据无法触发计算。

这种现象称之为watermark对齐（以最小值为准，不会以最大值为准）。

请问：这种默认的机制，好还是不好？

这种情况，当数据量过大时，会导致窗口内的数据堆积而无法正常处理。

因为它在等其他并行度的watermark时间到达临界点。但是万一其他的并行度的没有数据处理，而watermark迟迟滞后，

因此会导致整个窗口都没办法处理，数据大量堆积。

怎么办呢？

Flink通过设置**最大空闲等待时间**withIdleness来解决watermark对齐的问题。

### DataStream案例（withIdleness）

~~~java
package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: itcast
 * @date: 2023/8/19 20:10
 * @desc: 演示多并行度下设置最大空闲等待时间。
 */
public class Demo07_WithIdleness {
    public static void main(String[] args) throws Exception {
        //1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为2
        env.setParallelism(2);

        //2.数据源Source
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理Transformation
        //3.1 把数据源转换为水位传感器的类WaterSensor
        SingleOutputStreamOperator<WaterSensor03> mapDS = sourceDS.map(new MapFunction<String, WaterSensor03>() {
            @Override
            public WaterSensor03 map(String value) throws Exception {
                String[] lines = value.split(",");
                return new WaterSensor03(lines[0], Integer.valueOf(lines[1]), Long.valueOf(lines[2]));
            }
        });
        //3.2 给数据添加水印，这里指定为固定延迟水印，延迟时间为2秒钟，并且设置最大空闲等待时间
        SingleOutputStreamOperator<WaterSensor03> watermarkDS = mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor03>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                //设置最大空闲等待时间，这里设置为30s，也就是说，最多等你30秒钟
                .withIdleness(Duration.ofSeconds(30))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor03>() {
                    @Override
                    public long extractTimestamp(WaterSensor03 element, long recordTimestamp) {
                        System.out.println("线程号为：" + Thread.currentThread().getId() + "消费了事件时间为：" + element.getTs() + "的数据。");
                        return element.getTs() * 1000;
                    }
                }));
        //3.3 对数据按照id进行分组/分流
        SingleOutputStreamOperator<String> result = watermarkDS.keyBy(value -> value.getId())
                //3.4 划分窗口，这里指定为滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //3.5 调用偏底层的API（process）
                .process(new ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor03, String, String, TimeWindow>.Context context, Iterable<WaterSensor03> elements, Collector<String> out) throws Exception {
                        out.collect("窗口的key为：" + s +
                                "\n窗口内的数据为：" + elements +
                                "\n窗口内的数据量为：" + elements.spliterator().estimateSize() +
                                "\n窗口大小为：[" + context.window().getStart() + "," + context.window().getEnd() + ")" +
                                "\n当前的watermark时间为：" + context.currentWatermark());
                    }
                });

        //4.数据Sink
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

截图如下：

![1692448977722](assets/1692448977722.png)

结论：设置了最大空闲等待时间后，就算有其他并行度的数据没有达到窗口的临界时间，在最大空闲等待时间结束后，窗口仍然会触发计算。可以解决watermark对齐的问题。

### 面试题：Flink如何保证迟到数据不丢失

在FlinkSQL中，我们可以设置延迟时间，来保证绝大多少迟到数据会被正常处理。

对于异常迟到的数据，FlinkSQL无法保证。

在Flink DataStream API中，我们可以通过设置固定延迟水印、AllowLateness机制来处理绝大多数正常迟到数据的情况，

对于异常迟到数据，我们可以通过侧输出机制来捕获，不会让数据丢失。

对于多并行度下，我们可以设置最大空闲等待时间来解决watermark对齐的问题。

## Flink的Checkpoint

* 为什么学Checkpoint
* Checkpoint概述
* Checkpoint的执行流程
* 案例演示
* 重启策略
* 状态后端
* 案例演示

### 为什么学Checkpoint

Flink是做流计算的，流式计算是没有结束的，任务是不会终止的。

也就是说，如果任务出现异常，Flink是如何保证数据的一致性的。checkPoint保证数据的一致性

在流式计算中，Flink是通过Checkpoint机制来保证任务持续稳定运行的。

这就是我们学习Checkpoint机制的原因。

### Checkpoint概述

Checkpoint，又称为检查点机制，是为了Flink流式任务的容错的，也就是说，Flink流式任务在出现异常后，如何继续提供服务呢？

保证7*24小时不挂掉。

### Checkpoint的执行流程

![1692451824378](assets/1692451824378.png)

参考文字描述如下：

~~~shell
#1.检查点协调器（Checkpoint Coordinator）会定期发送一个一个的barrier（栅栏），这个barrier会混在数据中，随着数据流，一起流向source算子

#2.当Flink算子在处理数据时，正常处理就好

#3.当Flink算子在碰到Barrier（栅栏）时，它就会停下手里的工作，把当前的本地状态向主节点的检查点协调器汇报

#4.等状态汇报完后，这一个算子的Checkpoint就算做完了，Barrier会随着数据流，流向下一个算子

#5.下一个算子在接收到Barrier的时候，也会停下手里的工作，把本地的状态向主节点的检查点协调器进行汇报，以此类推...

#6.等Barrier把所有算子的状态都向主节点的检查点协调器汇报完后，这一轮的Checkpoint就算做完了

#7.等到下一个周期的时候，再继续下一个Checkpoint的状态汇报
~~~

> Checkpoint既然是用户自己设置的，一般设置多久合适呢？
>
> 答：一般建议分钟级别。不能太短。也不能太长。

### 思考：Flink有了Checkpoint就能实现容错吗？

不会。

如果任务怎么挂了怎么办。

也就是说，Checkpoint的全局状态，必须在任务没有挂掉的情况下才有用。

换言之，如果出现一些异常情况，必须保证Flink任务有一定的恢复策略才行。

Flink在遇到这种情况的时候，它内部提供了重启策略的机制，能够保证当出现问题的时候，进行自身恢复。

案例：

~~~java
package day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: itcast
 * @date: 2023/8/19 21:43
 * @desc: Checkpoint演示
 */
public class Demo08_Checkpoint {
    public static void main(String[] args) throws Exception {
        //1.够建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置重启策略，这里指定为固定延迟重启策略，如果不设置重启策略，则任务会挂掉
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1));

        //2.数据源
        DataStreamSource<String> sourceDS = env.socketTextStream("node1", 9999);

        //3.数据处理
        //对数据进行flatMap扁平化处理
        SingleOutputStreamOperator<String> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    if (word.equals("shit")) {
                        throw new Exception("请不要说脏话...");
                    } else {
                        out.collect(word);
                    }
                }
            }
        });

        //把单词转换为(单词,1)的形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //对单词进行分组/分流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapDS.keyBy(value -> value.f0)
                .sum(1);

        //4.数据输出
        result.print();

        //5.启动流式任务
        env.execute();
    }
}
~~~

### 重启策略











