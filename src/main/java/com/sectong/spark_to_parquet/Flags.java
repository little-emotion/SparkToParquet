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
	private String kafka_broker;
	private String kafka_topic;
	private String parquet_file;

	private boolean initialized = false;

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
			THE_INSTANCE.kafka_broker = cl.getOptionValue(AppMain.KAFKA_BROKER, "kafka:9092");
			THE_INSTANCE.kafka_topic = cl.getOptionValue(AppMain.KAFKA_TOPIC, "apache");
			THE_INSTANCE.parquet_file = cl.getOptionValue(AppMain.PARQUET_FILE, "/user/spark/");
			THE_INSTANCE.initialized = true;
		} catch (ParseException e) {
			THE_INSTANCE.initialized = false;
			System.err.println("Parsing failed.  Reason: " + e.getMessage());
		}
	}
}
