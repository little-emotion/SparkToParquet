package com.td.spark_to_parquet;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Writer {

   private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");

    public static void main(String[] args) throws InterruptedException {
        String parquetPath = "ods/td/";

        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("sparkToParquet");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5*60));
        SparkSession sparkSession = new SparkSession(jssc.sparkContext().sc());

        Map<String, Object> kafkaParams = new HashMap<>();
        //62.234.212.81
        kafkaParams.put("bootstrap.servers", "g1s0:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        //td_topic
        Collection<String> topics = Arrays.asList("topicB");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );


        stream.foreachRDD(rdd -> {
            Map<String, List<TDLog>> data2Log = new HashMap<>();
            rdd.map(record->{
                TDLog log = parseByLog(record.value());
                data2Log.putIfAbsent(parseDate(log.getTime()), new ArrayList<>()).add(log);
                return null;
                });

            for(String k : data2Log.keySet()){
                Dataset<Row> squaresDF = sparkSession.createDataFrame(data2Log.get(k), TDLog.class);
                squaresDF.write().mode("append").parquet(parquetPath+k);
            }

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            // some time later, after outputs have completed
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });

        jssc.start();
        jssc.awaitTermination();
    }

    private static TDLog parseByLog(String str){
        TDLog log = JSON.parseObject(str, new TypeReference<TDLog>() {});

        return log;
    }

//    public static void main(String[] args) throws InterruptedException {
//        TDLog log = new TDLog();
//        log.setAppName("taobao");
//        log.setDeviceId(1);
//        log.setTime(new Date().getTime());
//
//        TDLog hh = parseByLog(JSON.toJSONString(log));
//        System.out.println(hh.toString());
//        parseDate(new Date().getTime());
//    }

    private static String parseDate(long time){
        Date date = new Date(time);
        String formatStr =formatter.format(date);
        System.out.println(formatStr);
        return formatStr;
    }
}
