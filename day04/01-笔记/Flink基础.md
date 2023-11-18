# Flink基础

## 今晚课程内容介绍

* 系统架构（详细版）
    * JobManager
    * TaskManager
* 任务提交流程
    * 抽象层面（大致的流程）
    * Standalone
    * Yarn
* 一些重要的概念
    * 算子&算子链
    * 宽窄依赖
    * Job
    * Task
    * SubTask
    * 并行度&槽（slot）
    * Flink的四张图

需要对之前的内容有印象，能想起来才OK。

## 系统架构

环境变量配置：在`/etc/profile`文件中添加如下内容，保存退出。source一下即可。

~~~shell
#1.添加的内容
export FLINK_HOME=/export/server/flink
export PATH=$PATH:$FLINK_HOME/bin


#2.source一下
source /etc/profile
~~~

拿官方的图来解释：

![The processes involved in executing a Flink dataflow](assets/processes.svg)

### 通信系统

Spark：netty。

Flink：akka。

![1691668595751](assets/1691668595751.png)

上述地址就是akka的通信地址，这个地址不需要记住，能认识就行。

### 客户端

Flink的客户端只有提交任务的功能，提交完成之后，就和客户端没关联了。

这一点和Spark不同。Spark中Driver非常重要。是运行中任务的老大（AppMaster）

### JobManager

Flink的JobManager由以下三个子组件组成：

* JobMaster（调度）

~~~shell
负责准备资源，同事把任务调度到从节点运行
~~~

* ResourceManager（资源管理）

~~~shell
它和Yarn的ResourceManager没有任何关系。它只是叫这个名字而已。
Resourcemanager是Flink的资源管理组件，为即将运行的任务提供资源。
资源从何而来？ 看是什么运行模式。
（1）Standalone模式，问从节点TaskManager要资源
（2）Yarn模式，问Yarn的ResourceManager要资源。

独立模式：资源是固定的
Yarn模式：资源是可以动态分配的
~~~

* Dispatcher（分发器）

~~~shell
分发器，能启动WebUI。在有些模式下并没有。它不是核心。
哪些模式有：
Standalone，有专门8081WebUI端口
Yarn Session模式，有专门的WebUI端口，这个端口是随机的。

哪些模式没有：
Yarn Per-job模式
Yarn Application模式
~~~

* 功能

管理从节点，调度任务，并且监控任务运行，容错等。

### TaskManager

TaskManager，从节点，它是负责任务的执行。通过slot槽的方式来运行任务。

slot：是均分从节点内存的。

### Scheduler

Spark的调度：

~~~shell
DAGScheduler：逻辑调度，划分Job、Stage、Task等
TaskScheduler：物理调度，哪个Task在哪个executor上执行。

举例：
逻辑调度：领导今晚请吃饭。
物理调度：今晚吃啥（菜系），多少人，能不能带家属，预算多少等
~~~

Flink的调度：

就是负责任务的调度执行。

### Checkpoint

容错，能保证流式任务正常运行。Flink的容错机制，是Flink中的核心之一，后面再详细解释。

### 网络管理器

从节点与从节点之间，需要进行数据传输的时候，数据需要经过跨网络传输，这是通过网络管理器来管理的。

### 内存&IO管理器

负责各个从节点的内存和IO的管理。

## 任务提交流程

### 抽象提交流程

抽象提交流程，指的是，不管是什么运行模式，大体上都是这个流程。

![1691670301680](assets/1691670301680.png)

文字参考如下：

~~~shell
#1.任务提交给JobManager的Dispatcher（分发器）

#2.Dispatcher（分发器）会把任务提交给JobMaster进行调度

#3.JobMaster收到任务后，会为任务资源，进行调度

#4.JobMaster会向JobManager的ResourceManager要资源

#5.不管如何，ResourceManger会向JobMaster提供资源

#6.JobMaster有了资源后，会把任务调度给相应的从节点执行

#7.如果任务支持完成，则资源会释放
~~~

### Standalone模式提交流程

图示如下：

![1691671910338](assets/1691671910338.png)

文字参考如下：

~~~shell
#1.客户端把任务提交给JobManager的Dispatcher（分发器）

#2.分发器（Dispatcher）会启动JobMaster，把任务提交给JobMaster来进行调度运行

#3.JobMaster会向ResourceManager申请资源

#4.ResourceManager会向TaskManager请求

#5.TaskManager会向JobMaster提供资源

#6.JobMaster有了资源后，会把任务调度给TaskManager上执行

#7.如果任务执行完成，则资源会释放
~~~

### Yarn模式提交流程

Flink On Yarn有三种运行模式：

* Session模式
* Per-job模式
* Application模式

#### Session模式

这种模式有两步：

~~~shell
#1.启动Session集群
yarn-session.sh

#2.提交任务
提交任务的脚本
~~~

##### 启动Session集群

![1691672288451](assets/1691672288451.png)

文字描述如下：

~~~shell
#1.脚本会向Yarn的ResourceManager启动一个Container（AppMaster），由于这里是Flink的应用，这个AppMaster也是Flink的主节点（JobManager）

#2.这个时候，由于没有任务提交，因此没有动态初始化从节点，同时这个主节点也只有分发器和资源管理器
~~~

##### 提交任务

![1691672522489](assets/1691672522489.png)

文字描述如下：

~~~shell
#1.任务会提交给分发器

#2.分发器会启动JobMaster（调度）

#3.JobMaster会向JobManager的ResourceManager申请资源

#4.JobManager会向Yarn的ResourceManager申请资源

#5.Yarn收到请求后，会动态初始化一批Container（容器），这些容器启动后，会反向注册给APpMaster（JobManager）

#6.JobMaster有了资源后，会把任务调度到相应的容器中（TaskManager的从节点）运行

#7.如果任务跑完，则资源会释放，容器（从节点）会被JobManager注销，JobManager不会注销
~~~

#### Per-job&Application模式

![1691672844250](assets/1691672844250.png)

我们以Application模式为例（per-job模式在1.15版本中已经被废弃了），描述一下任务的执行流程：

~~~shell
#1.客户端提任务到集群中运行

#2.Yarn的ResourceManager收到任务后，会启动一个容器（Container），也就是APpMaster，也就是JobManager

#3.任务会提交给JobManager的JobMaster调度执行

#4.JobMaster会向JobManager申请资源，但是JobManager没有资源

#5.JobManager会向Yarn的ResourceManager申请资源来运行任务

#6.Yarn收到请求后，会动态初始化一批容器（TaskManager），这一批容器启动会会反向注册到AppMaster（JobManager）

#7.JobManager的JobMaster收到资源后，会将任务调度给相应的容器中运行

#8.如果任务运行完成，则资源会资源，TaskManager会被AppMaster申请注销，AppMaster会向Yarn的ResourceManager申请将自己注销
~~~

仅供参考，理解之后，用自己的话能表达出来即可。

## 一些重要的概念

![1691675905209](assets/1691675905209.png)

- 算子&算子链

~~~shell
算子：计算方法
算子链：把窄依赖内部的算子串起来执行
~~~

- 宽窄依赖

~~~shell
宽依赖：redistribution Dependency，宽依赖（DataStream是一对多的）
窄依赖：one-to-one Dependency，窄依赖（DataStream是一对一的）
~~~

- Job

~~~shell
Flink提交的作业
~~~

- Task

~~~shell
根据宽依赖划分的任务
~~~

- SubTask

~~~shell
一个Task中有多个子任务，SubTask就是子任务
~~~

- 并行度&槽（slot）

~~~shell
#1.Flink可以有四种并行度的设置方式：
（1）配置文件中，默认的并行度，优先级最低
（2）任务提交时，常用的，推荐使用
（3）环境层面，全局环境中设置
（4）算子层面，在某一个算子时可以修改并行度，优先级最高

#2.slot，槽
slot，槽，是静态的资源单位。

并行度的数量 <= slot的数量
~~~

- Flink的四张图

~~~shell
#1.数据流图
Dataflow Graph

#2.任务图
Job Graph

#3.执行图
Execution Graph

#4.物理图
Physical Graph
~~~

* 资源分配的原则

~~~shell
把不同SubTask下的算子，会尽量放在同一个slot中执行。为了减少数据传输，提升运行效率。
把相同SubTask下的算子，会尽量放在不同的slot中执行。为了重复利用集群资源，提升并行计算的能力。
~~~

