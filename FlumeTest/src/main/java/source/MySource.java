package source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String field;
    private Long delay;

    @Override
    public Status process() throws EventDeliveryException {

        HashMap<String, String> headerMap = new HashMap<>();

        SimpleEvent simpleEvent = new SimpleEvent();

        simpleEvent.setHeaders(headerMap);

        for (int i = 0; i < 5; i++){
            simpleEvent.setBody((field+i).getBytes(StandardCharsets.UTF_8));

            getChannelProcessor().processEvent(simpleEvent);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return Status.BACKOFF;
            }
        }

        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        this.delay = context.getLong("delay");
        this.field = context.getString("field","hello");
    }
}
