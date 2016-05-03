package com.sectong.spark_to_parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.collection.Seq;

/**
 * 运行程序，spark-submit --class "com.sectong.spark_to_parquet.AppMain" --master
 * yarn target/park_to_parquet-0.0.1-SNAPSHOT.jar --kafka_broker
 * hadoop1:6667,hadoop2:6667 --kafka_topic apache --parquet_file /user/spark/
 * --slide_interval 30
 */
public class AppMain {

	public static final String WINDOW_LENGTH = "window_length";
	public static final String SLIDE_INTERVAL = "slide_interval";
	public static final String KAFKA_BROKER = "kafka_broker";
	public static final String KAFKA_TOPIC = "kafka_topic";
	public static final String PARQUET_FILE = "parquet_file";

	private static final Options THE_OPTIONS = createOptions();

	private static Options createOptions() {
		Options options = new Options();

		options.addOption(new Option(WINDOW_LENGTH, true, "The window length in seconds"));// 窗口大小
		options.addOption(new Option(SLIDE_INTERVAL, true, "The slide interval in seconds"));// 计算间隔
		options.addOption(new Option(KAFKA_BROKER, true, "The kafka broker list")); // Kafka队列
		options.addOption(new Option(KAFKA_TOPIC, true, "The kafka topic"));// TOPIC
		options.addOption(new Option(PARQUET_FILE, true, "The parquet file"));// 写入Parquet文件位置
		return options;
	}

	public static void main(String[] args) throws IOException {
		Flags.setFromCommandLineArgs(THE_OPTIONS, args);

		// 初始化Spark Conf.
		SparkConf conf = new SparkConf().setAppName("A SECTONG Application: Apache Log Analysis with Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Flags.getInstance().getSlideInterval());
		SQLContext sqlContext = new SQLContext(sc);

		// 初始化参数
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(Flags.getInstance().getKafka_topic().split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", Flags.getInstance().getKafka_broker());

		// 从Kafka Stream获取数据
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 5266880065425088203L;

			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<ApacheAccessLog> accessLogsDStream = lines.flatMap(line -> {
			List<ApacheAccessLog> list = new ArrayList<>();
			try {
				// 映射每一行
				list.add(ApacheAccessLog.parseFromLogLine(line));
				return list;
			} catch (RuntimeException e) {
				return list;
			}
		}).cache();

		accessLogsDStream.foreachRDD(rdd -> {

			// rdd to DataFrame
			DataFrame df = sqlContext.createDataFrame(rdd, ApacheAccessLog.class);
			// 写入Parquet文件
			df.write().partitionBy("ipAddress", "method", "responseCode").mode(SaveMode.Append).parquet(Flags.getInstance().getParquetFile());

			return null;
		});

		// 启动Streaming服务器
		jssc.start(); // 启动计算
		jssc.awaitTermination(); // 等待终止
	}
}
