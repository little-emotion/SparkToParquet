# 有一个设想

当有持续不断的结构化或非结构化大数据集以流（streaming）的方式进入分布式计算平台，
能够保存在大规模分布式存储上，并且能够提供准实时SQL查询，这个系统多少人求之不得。

今天，咱们就来介绍一下这个计算框架和过程。


# 问题分解一下

## 数据哪里来？

假设，你已经有一个数据收集的引擎或工具（不在本博客讨论范围内，请出门左转Google右转百度），怎么都行，
反正数据能以流的方式给出来，塞进Kafka类似的消息系统。

## 结构化？非结构化？如何识别业务信息？

关于结构化或非结构化，也不在今天的主要讨论范围，但是，必须要说明的是，
你的数据能够以某种规则进行正则化，比如：空格分隔，CSV，JSON等。咱们今天以Apache网站日志数据作为参照。

类似如下：

```

	124.67.32.161 - - [10/Apr/2016:05:37:36 +0800] "GET /blog/app_backend.html HTTP/1.1" 200 26450
```

## 如何处理？写到哪里去？

拿到数据，我们需要一些处理，将业务逻辑分离开来，做成二维表，行列分明，就像是关系型数据库的表。这个事情有Spark DataFrame来完成。

就像写入关系型数据库一样，我们需要将DataFrame写入某处，这里，就是Parquet文件，天然支持schema，太棒了。

## 怎么取出来？还能是SQL？

我们的数据已经被当做“二维表，Table”写入了Parquet，取出来当然也得是“表”或其他什么的，当然最好是能暴露出JDBC SQL，相关人员使用起来就方便了。

这个事情交给Spark的 SparkThriftServer 来完成。

# 设计蓝图

以上分解似乎完美，一起来看看“设计框架”或“蓝图”。

![](spark_to_parquet/spark_to_parquet.png)

算了，不解释了，图，自己看。

# Coding Style

## 从Kafka Stream获取数据
```

	// 从Kafka Stream获取数据
	JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
			StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);


```

## 写入Parquet

```

	accessLogsDStream.foreachRDD(rdd -> {
		// 如果DF不为空，写入(增加模式)到Parquet文件
		DataFrame df = sqlContext.createDataFrame(rdd, ApacheAccessLog.class);
		if (df.count() > 0) {
			df.write().mode(SaveMode.Append).parquet(Flags.getInstance().getParquetFile());
		}
		return null;
	});

```

## 创建Hive表

### 使用spark-shell，获取Parquet文件, 写入一个临时表;

scala代码如下：

```

	import sqlContext.implicits._
	val parquetFile = sqlContext.read.parquet("/user/spark/apachelog.parquet")
	parquetFile.registerTempTable("logs")

```

### 复制schema到新表链接到Parquet文件。

在Hive中复制表，这里你会发现，文件LOCATION位置还是原来的路径，目的就是这个，使得新写入的文件还在Hive模型中。

我总觉得这个方法有问题，是不是哪位Hive高人指点一下，有没有更好的办法来完成这个工作？

```

	CREATE EXTERNAL TABLE apachelog LIKE logs STORED AS PARQUET LOCATION '/user/spark/apachelog.parquet';

```

## 启动你的SparkThriftServer

当然，在集群中启用ThriftServer是必须的工作，SparkThriftServer其实暴露的是Hive2服务器，用JDBC驱动就可以访问了。


# 我们都想要的结果

本博客中使用的SQL查询工具是[SQuirreL SQL](http://squirrel-sql.sourceforge.net)，具体JDBC配置方法请参照前面说的向左向右转。

![](spark_to_parquet/squirrel-sql.png)

结果看似简单，但是经历还是很有挑战的。
