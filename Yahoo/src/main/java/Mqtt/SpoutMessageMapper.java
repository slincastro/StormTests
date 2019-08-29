package Mqtt;

import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.MqttMessageMapper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SpoutMessageMapper implements MqttMessageMapper {
    public Values toValues(MqttMessage mqttMessage) {
        String topic = mqttMessage.getTopic();
        String[] topicElements = topic.split("/");
        String[] payloadElements = new String(mqttMessage.getMessage()).split("/");

        return new Values(payloadElements[0], payloadElements[1]);
    }

    public Fields outputFields() {
        return new Fields("id", "temperature");
    }
}
