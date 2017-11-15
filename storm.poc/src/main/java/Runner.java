import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


public class Runner extends StormTopology {

    public static void main(String[] args) {
        setup(args);
    }

    private static void setup(String[] args) {
        try {
            Config config = new Config();
            config.setNumWorkers(1);
            config.setMaxSpoutPending(Integer.MAX_VALUE);
//            config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8);
            config.setMessageTimeoutSecs(100000);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm_poc_topology", config, buildTopology(args));
            // Time to let topology run for
//            Thread.sleep(100000);
//            cluster.killTopology("storm_poc_topology");
            // Time for cleanup
//            Thread.sleep(100000);
        } catch (Exception e) {
            System.out.println("Encountered exception while starting topology. Exiting." + e.getMessage());
            System.exit(1);
        }
//        System.exit(0);
    }

    private static StormTopology buildTopology(String[] args) {
        MessageProcessingTopologyComponents.MessageSpout spout = new MessageProcessingTopologyComponents.MessageSpout();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("message_spout", new MessageProcessingTopologyComponents.MessageSpout(), 1);
        builder.setBolt("message_bolt", new MessageProcessingTopologyComponents.MessageBolt(), 2).shuffleGrouping("message_spout");
        return builder.createTopology();
    }
}
