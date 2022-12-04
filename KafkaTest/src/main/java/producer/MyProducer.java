package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {
        Properties prop = new Properties();


        prop.put("bootstrap.servers","node01:9092");
        prop.put("acks","all");
        prop.put("retries",3);

        prop.put("batch.size",16384);
        prop.put("linger.ms",1);

        prop.put("buffer.memory",33554432);

        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i< 100; i++){
            producer.send(new ProducerRecord<String,String>("first",Integer.toString(i)));


        }

        producer.close();

    }
}
