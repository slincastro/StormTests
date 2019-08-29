package Mqtt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class MqttBolt extends BaseBasicBolt {


    private String filename;

    public void prepare(Map<String,Object> conf,
                        TopologyContext context){
        filename = conf.get("fileToWrite").toString();

    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String id = tuple.getValueByField("id").toString();
        int temperature = Integer.parseInt(tuple.getValueByField("temperature").toString());

        basicOutputCollector.emit(new Values(id, temperature));

        String expresion = temperature > 18 ? "Arraray" : "Chachay";
        PrintWriter writer = null;

        try {
            writer = new PrintWriter(filename, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        writer.append(id +" says :" + expresion);
        writer.append("\n");
        writer.close();

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "temperature"));
    }

}
