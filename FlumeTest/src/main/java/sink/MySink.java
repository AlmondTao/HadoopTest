package sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class MySink extends AbstractSink implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(MySink.class);

    private String prefix;
    private String suffix;

    @Override
    public Status process() throws EventDeliveryException {

        Event take;


        Channel channel = getChannel();

        Transaction transaction = channel.getTransaction();

        try{
            transaction.begin();

            while (true){
                take = channel.take();
                if (take != null){
                    break;
                }


            }

            log.info(prefix+new String(take.getBody(), Charset.defaultCharset())+suffix);

            transaction.commit();
            return Status.READY;
        } finally {
            transaction.close();
        }



    }

    @Override
    public void configure(Context context) {
        this.prefix = context.getString("prefix");
        this.suffix = context.getString("suffix","defaultSuffix");
    }
}
