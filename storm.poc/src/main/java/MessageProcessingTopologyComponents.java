import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MessageProcessingTopologyComponents {

    public static class MessageBolt extends BaseRichBolt {

        TopologyContext context;
        OutputCollector collector;
        Map<Integer, Long> sleepCycles;
        static Map<Integer, AtomicInteger> processingCount = new ConcurrentHashMap<>();

        public MessageBolt() {
            sleepCycles = new HashMap<>();
            sleepCycles.put(1, 50L);
            sleepCycles.put(2, 100L);
            sleepCycles.put(3, 200L);
            sleepCycles.put(4, 400L);
            sleepCycles.put(5, 500L);
            sleepCycles.put(6, 600L);
            sleepCycles.put(7, 700L);
            sleepCycles.put(8, 800L);
            sleepCycles.put(9, 900L);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            context = topologyContext;
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String message = tuple.getString(0);
            long sleepInterval = sleepCycles.get(context.getThisTaskId() % 10);
            System.out.println("Executor " + context.getThisTaskId() + " received message " + message);
            if (context.getThisTaskId() == 2 && Integer.parseInt(message) < 5) { // Sleeping but just for a single message.
                System.out.println("I am executor2. I'm going to sleep for 60 seconds now.");
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("I am executor2. I'm waking up now !");
            }

            collector.ack(tuple);
            updateProcessingCount(context.getThisTaskId());
            System.out.println("Executor " + context.getThisTaskId() + " processed message: " + message + ". Sleeping for " + 2000 + "millis");
//            if (context.getThisTaskId() == 2 && (processingCount.get(2).intValue() % 100 == 0)) {
//            if (true) {
//                printProcessingCountMap();
//            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // Do Nothing
            }
        }

        static void updateProcessingCount(int taskId) {
            if (processingCount.containsKey(taskId)) {
                processingCount.get(taskId).incrementAndGet();
            } else {
                processingCount.put(taskId, new AtomicInteger());
            }
        }

        static void printProcessingCountMap() {
            System.out.println("Processing Map ----------------START------------------------------------------------------>");
            for (int key : processingCount.keySet()) {
                System.out.println("Executor " + key + " : Count " + processingCount.get(key));
            }
            System.out.println("Processing Map ----------------END-------------------------------------------------------->");
        }

        @Override
        public void cleanup() {
            System.out.println("Cleaning up bolt " + context.getThisTaskId());
            System.out.println(" ----------------CLEANUP------------------------------------------------------>");
            printProcessingCountMap();
            System.out.println(" ----------------CLEANUP------------------------------------------------------>");
        }
    }

    public static class MessageSpout extends BaseRichSpout {

        int messageCount;
        int taskId;
        SpoutOutputCollector _collector;

        TopologyContext context;
        private AtomicLong currentCount = new AtomicLong(0);
        private long lastTimestamp = System.currentTimeMillis();
        private long lastCount =0;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.context = context;
            taskId = context.getThisTaskId();
            _collector = collector;
            System.out.println("Initialised spout with taskId " + taskId);
        }

        @Override
        public void nextTuple() {
            _collector.emit(new Values(Integer.toString(messageCount)), messageCount);
            System.out.println("--------Spout Emitting message: " + messageCount);
            messageCount++;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // Do Nothing.
            }
//            currentCount.incrementAndGet();
//            if(System.currentTimeMillis()-lastTimestamp>1000){
//                System.out.println("Current count is "+(System.currentTimeMillis()-lastTimestamp)+" count "+(currentCount.get()-lastCount));
//                lastTimestamp=System.currentTimeMillis();
//                lastCount=currentCount.get();
//            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("message"));
        }

        @Override
        public void close() {
            System.out.println("Cleaning up spout " + context.getThisTaskId() + ". Message count: " + messageCount);
        }
    }
}