package com.sectong.spark_to_parquet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.spark.streaming.Duration;

public class Flags {
	private static Flags THE_INSTANCE = new Flags();

	private Duration windowLength;
	private Duration slideInterval;
	private static String kafka_broker = "g1s0:9092";//kafka服务器地址
	private static String kafka_topic = "td_topic";//kafka消息类别
	private String parquet_file;

	private boolean initialized = false;

//	public static String bootstrapServers = "g1s0:9092";//kafka服务器地址
//	public static String topic = "td_topic";//kafka消息类别

	private Flags() {
	}

	public Duration getWindowLength() {
		return windowLength;
	}

	public Duration getSlideInterval() {
		return slideInterval;
	}

	public String getKafka_broker() {
		return kafka_broker;
	}

	public String getKafka_topic() {
		return kafka_topic;
	}

	public String getParquetFile() {
		return parquet_file;
	}

	public static Flags getInstance() {
		if (!THE_INSTANCE.initialized) {
			throw new RuntimeException("Flags have not been initalized");
		}
		return THE_INSTANCE;
	}

	public static void setFromCommandLineArgs(Options options, String[] args) {


		CommandLineParser parser = new PosixParser();
		try {
			CommandLine cl = parser.parse(options, args);
			// 参数默认值
			THE_INSTANCE.windowLength = new Duration(
					Integer.parseInt(cl.getOptionValue(AppMain.WINDOW_LENGTH, "30")) * 1000);
			THE_INSTANCE.slideInterval = new Duration(
					Integer.parseInt(cl.getOptionValue(AppMain.SLIDE_INTERVAL, "5")) * 1000);
			THE_INSTANCE.kafka_broker = cl.getOptionValue(AppMain.KAFKA_BROKER, kafka_broker);
			THE_INSTANCE.kafka_topic = cl.getOptionValue(AppMain.KAFKA_TOPIC, kafka_topic);
			THE_INSTANCE.parquet_file = cl.getOptionValue(AppMain.PARQUET_FILE, "/user/spark/");
			THE_INSTANCE.initialized = true;
		} catch (ParseException e) {
			THE_INSTANCE.initialized = false;
			System.err.println("Parsing failed.  Reason: " + e.getMessage());
		}
	}
}
