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

public class YahooBolt extends BaseBasicBolt {

    private PrintWriter writer;

    public void prepare(Map<String,Object> conf,
                        TopologyContext context){
        String filename = conf.get("fileToWrite").toString();
        try{
            this.writer = new PrintWriter(filename, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String symbol = tuple.getValue(0).toString();
        String timestamp = tuple.getString(1);

        Double price = (Double) tuple.getValueByField("Price");
        Double prevClose = tuple.getDoubleByField("prev_close");

        boolean gain = price>prevClose;

        basicOutputCollector.emit(new Values(symbol, timestamp, price, gain));
        writer.println(symbol +", "+ timestamp +", "+price+", "+gain);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company", "timestamp", "price", "gain"));
    }

    public void cleanup() {
        writer.close();
    }

}
