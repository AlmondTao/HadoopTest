package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyInterceptor implements ProducerInterceptor<String,String> {

    private int errorCount = 0;
    private int successCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        if (record.key() != null && record.value() != null){
            System.out.println("value:"+ record.value()+",key:"+record.key());
        }
        return new ProducerRecord<>(record.topic(),record.partition(),record.timestamp(),record.key(),System.currentTimeMillis() + "," + record.value().toString());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            errorCount++;
        }else{
            successCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("Successful sent: " + successCount);
        System.out.println("Failed sent: " + errorCount);

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
