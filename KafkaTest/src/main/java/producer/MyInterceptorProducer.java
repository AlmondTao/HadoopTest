package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MyInterceptorProducer {
    public static void main(String[] args) {
        Properties prop = new Properties();


        prop.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        prop.put("acks","all");
        prop.put("retries",3);

        prop.put("batch.size",16384);
        prop.put("linger.ms",1);

        prop.put("buffer.memory",33554432);

        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        List<String> interceptorList = new ArrayList<>();
        interceptorList.add("interceptor.MyInterceptor");
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptorList);

        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);


        for (int i = 0; i< 100; i++){
            producer.send(new ProducerRecord<String,String>("first",Integer.toString(i)));


        }

        producer.close();

    }
}
