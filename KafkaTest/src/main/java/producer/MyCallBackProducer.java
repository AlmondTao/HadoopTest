package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.LogManager;

import java.util.Properties;

public class MyCallBackProducer {
   private static Logger log = LogManager.getLogger(MyCallBackProducer.class);

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

        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i< 10; i++){
            producer.send(new ProducerRecord<String, String>("second", Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        log.error(e);
                    }else {
                        log.warn("消息推送完成");
                        System.out.println("消息推送完成 =>"+recordMetadata.offset());
                    }
                }
            });


        }

        producer.close();

    }
}
