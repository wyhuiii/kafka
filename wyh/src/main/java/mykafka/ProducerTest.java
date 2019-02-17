package mykafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerTest {

    public static void main(String[] args) throws  Exception{
    	Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.184.128:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> procuder = new KafkaProducer<String, String>(props);

        String topic = "testtopic";
        for (int i = 1; i <= 2; i++) {
            String value = " this is another message_" + i;
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,i+"",value);
            procuder.send(record);
            System.out.println(i+" ----   success");
            Thread.sleep(1000);
        }
        System.out.println("send message over.");
        procuder.close();
    }

}
