package interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;


public class MyInterceptor implements Interceptor {


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String s = new String(body, Charset.forName("UTF-8"));
        if (s != null && s.endsWith("file")){
            event.getHeaders().put("type","file");
        }else if(s != null && s.endsWith("log")) {
            event.getHeaders().put("type","log");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event e : list){
            intercept(e);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Interceptor.Builder{
        Logger logger = LoggerFactory.getLogger(MyBuilder.class);
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {
            logger.info(context.toString());
        }
    }
}
