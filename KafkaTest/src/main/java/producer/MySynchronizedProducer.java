package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MySynchronizedProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties prop = new Properties();


        prop.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        prop.put("acks","0");
        prop.put("retries",3);

        prop.put("batch.size",16384);
        prop.put("linger.ms",1);

        prop.put("buffer.memory",33554432);

        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<>(prop);


        for (int i = 0; i< 20; i++){
            Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>("second", "synchronizedMessage"+i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {

                        System.out.println("消息推送完成 =>offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition());
                    }
                }
            });

            RecordMetadata recordMetadata = send.get();
            System.out.println("消息推送future =>offset:" + recordMetadata.offset() + ",partition:" + recordMetadata.partition());
        }


        producer.close();

    }

}
