import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Yahoo-finance-Spout", new YahooSpout());
        builder.setBolt("Yahoo-finance-Bolt", new YahooBolt())
                .shuffleGrouping("Yahoo-finance-Spout");

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToWrite", "/Users/scastro/Projects/StormTests/output.txt");

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Stock-Tracker-Topology", conf, topology);
            Thread.sleep(10000);
        }
        finally {
            cluster.shutdown();
        }

    }
}