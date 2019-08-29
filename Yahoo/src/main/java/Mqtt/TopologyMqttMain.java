package Mqtt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.mqtt.common.MqttOptions;
import org.apache.storm.mqtt.mappers.StringMessageMapper;
import org.apache.storm.mqtt.spout.MqttSpout;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;

public class TopologyMqttMain {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        MqttOptions options = new MqttOptions();
        options.setTopics(Arrays.asList("myfirst/test"));
        options.setCleanConnection(false);

        MqttSpout spout = new MqttSpout(new SpoutMessageMapper(), options);


        builder.setSpout("mqtt-spout", spout);
        builder.setBolt("mqtt-bolt", new MqttBolt()).shuffleGrouping("mqtt-spout");

        StormTopology mqttTopology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToWrite", "/Users/scastro/Projects/StormTests/MqttOutput.txt");

        try{
            cluster.submitTopology("Events-Topology", conf, mqttTopology);
            //Thread.sleep(10000);
        }
        finally {
            //cluster.shutdown();
        }

    }
}
