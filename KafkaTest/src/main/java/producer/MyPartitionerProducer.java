package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import partitioner.MyPartitioner;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyPartitionerProducer {
    private static Logger log = LogManager.getLogger(MyPartitionerProducer.class);
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

//        prop.put("partitioner.class", MyPartitioner.class);
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "partitioner.MyPartitioner");
        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i< 50; i++){
            String message;
            if (i%2 == 0){
                message = "tqy";
            }else{
                message = "defaultRoot";
            }
            producer.send(new ProducerRecord<String, String>("third", message + Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        log.error(e);
                    } else {
                        log.warn("消息推送完成");
                        System.out.println("消息推送完成 =>offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition());
                    }
                }
            });


        }

        producer.close();

    }
}
