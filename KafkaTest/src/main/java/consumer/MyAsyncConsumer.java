package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class MyAsyncConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");

        props.put("group.id","myConsumer");
        props.put("enable.auto.commit","false");
        props.put("auto.commit.interval.ms","1000");
        props.put("max.poll.records",10);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("first"));

        while (true){
//            ConsumerRecords<String, String> poll = consumer.poll(100);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));

            for (ConsumerRecord<String, String> record : records){

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Commit failed for" + offsets);
                    }else{
                        System.out.println("消费异步提交提交");
                    }

                }
            });
        }


    }
}
