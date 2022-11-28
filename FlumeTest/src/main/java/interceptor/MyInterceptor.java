package interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

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
            event.getHeaders().put("type","file");
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
}
