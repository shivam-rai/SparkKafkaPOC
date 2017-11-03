package com.flipkart.gap;

import com.flipkart.gap.pojos.Range;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleSparkJob {

    private static final String[] topicsList = "poc_topic_1,poc_topic_2,poc_topic_3".split(",");

    private JavaSparkContext sparkContext;
    private OffsetManager offsetManager;
    private Map<String, Object> kafkaParams;

    private void run(String[] args) {
        // Configuration
        kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", args[0]);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "poc_1");
        kafkaParams.put("auto.offset.reset", "earliest");

        // Initialisation
        offsetManager = new OffsetManager(topicsList, Integer.valueOf(args[1]), Integer.valueOf(args[2]));
        sparkContext = new JavaSparkContext(new SparkConf().setAppName("POC").setMaster("local[1]"));

        // Let's assume we need to start from epoch 0, till the time our OffsetManager starts giving NULL
        // Also, let's assume the Journey we're running needs events from all three topics.
        long currentEpoch = 1;
        while(true) {
            List<OffsetRange> offsetsForEpoch = new ArrayList<>();
            for (String topicName : topicsList) {
                Range offsetRange = offsetManager.getOffsetRangeForEpoch(topicName, currentEpoch);
                if (offsetRange != null && offsetRange.getStartingOffset() != offsetRange.getEndingOffset()) {
                    offsetsForEpoch.add(OffsetRange.create(topicName, 0, offsetRange.getStartingOffset(), offsetRange.getEndingOffset()));
                }
            }
            if (offsetsForEpoch.size() == 0) {
                System.out.println("We've processed all epochs, it seems. Current Epoch: " + currentEpoch);
                break;
            }
            OffsetRange[] offsetRangeList = new OffsetRange[offsetsForEpoch.size()];
            offsetRangeList = offsetsForEpoch.toArray(offsetRangeList);
            JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(sparkContext, kafkaParams, offsetRangeList, LocationStrategies.PreferConsistent());
            processRdd(rdd);
            // Finally
            currentEpoch++;
        }

    }

    private void processRdd(JavaRDD<ConsumerRecord<String, String>> rdd) {
        System.out.println("=======Processing RDD with ID: " + rdd.id() + "=======");
        rdd.foreach(record -> System.out.println(rdd.id() + "__" + record.value()));
    }

    public static void main(String[] args) {
        SampleSparkJob sampleSparkJob = new SampleSparkJob();
        sampleSparkJob.run(args);
    }
}
