package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyFlumeConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");

        props.put("group.id","flumeConsumer");
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("flumeTopic"));

        while (true){
//            ConsumerRecords<String, String> poll = consumer.poll(100);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));

            for (ConsumerRecord<String, String> record : records){

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }


    }
}
