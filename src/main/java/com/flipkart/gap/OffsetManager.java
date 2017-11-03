package com.flipkart.gap;

import com.flipkart.gap.pojos.EpochOffsetTuple;
import com.flipkart.gap.pojos.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * POC for seeing if we can read specified offsets from multiple topics, between Kafka and Spark.
 * For simplification, we're assuming there exists n topics, each with equal no. of messages.
 * We assume each time-unit can accommodate m messages. This is used to create a registry of offsets versus time-offset.
 * For simplification, let's assume each topic has a single partition.
 */
public class OffsetManager
{
    // Configuration
    private String[] topicsList;
    private int maxOffsetPerTopic; // Assuming equal messages per partition
    private int messagesPerTimeUnit;

    // Declarations
    private Map<String, List<EpochOffsetTuple>> offsetRegistry;


    OffsetManager(String[] topicsList, int maxOffsetPerTopic, int messagesPerTimeUnit) {
        this.topicsList = topicsList;
        this.maxOffsetPerTopic = maxOffsetPerTopic;
        this.messagesPerTimeUnit = messagesPerTimeUnit;
        populateOffsetRegistry();
        printOffsetRanges();
    }

    Range getOffsetRangeForEpoch(String topicName, long epoch) {
        List<EpochOffsetTuple> topicOffsetList = offsetRegistry.get(topicName);
        if (topicOffsetList == null || topicOffsetList.isEmpty()) {
            return null;
        }
        Iterator<EpochOffsetTuple> iterator = topicOffsetList.iterator();
        while (iterator.hasNext()) {
            EpochOffsetTuple epochOffsetTuple = iterator.next();
            if (epochOffsetTuple.getEpoch() == epoch) {
                long startingOffset = epochOffsetTuple.getOffset();
                if (iterator.hasNext()) {
                    EpochOffsetTuple nextEpochOffsetTuple = iterator.next();
                    return new Range(startingOffset, nextEpochOffsetTuple.getOffset());
                } else {
                    return new Range(startingOffset, maxOffsetPerTopic);
                }
            }
        }
        return null;
    }

    private void printOffsetRanges() {
        for (String topicName : topicsList) {
            System.out.println("Printing ranges for topic " + topicName);
            for (int i = 1; i <= (maxOffsetPerTopic /messagesPerTimeUnit); i++) {
                Range offsetRange = getOffsetRangeForEpoch(topicName, i);
                System.out.println("    Epoch: " + i + "    " + (offsetRange == null ? "  NULL" : "   " + offsetRange.toString()));
            }
        }
    }

    private void populateOffsetRegistry() {
        long startingEpoch = 1;
        long startingOffset = 0;

        offsetRegistry = new HashMap<>();
        for (String topic : topicsList) {
            long currentEpoch = startingEpoch;
            List<EpochOffsetTuple> epochOffsetTupleList = new ArrayList<>();
            for (long i = startingOffset; i < maxOffsetPerTopic; i+=messagesPerTimeUnit, currentEpoch++) {
                EpochOffsetTuple epochOffsetTuple = new EpochOffsetTuple(currentEpoch, i);
                epochOffsetTupleList.add(epochOffsetTuple);
            }
            offsetRegistry.put(topic, epochOffsetTupleList);
        }
        System.out.println("Offset Registry:");
        for (String topicName : offsetRegistry.keySet()) {
            System.out.println(topicName + ":");
            for (EpochOffsetTuple epochOffsetTuple : offsetRegistry.get(topicName)) {
                System.out.println("    " + epochOffsetTuple.toString());
            }
        }
    }
}
