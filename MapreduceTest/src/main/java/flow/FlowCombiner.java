package flow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/10/25
 */
public class FlowCombiner extends Reducer<Text,FlowWritable,Text,FlowWritable> {

    @Override
    protected void reduce(Text key, Iterable<FlowWritable> values, Context context) throws IOException, InterruptedException {
        FlowWritable flowWritable = new FlowWritable(key.toString(),0,0,0,0);

        for (FlowWritable flow : values){
            flowWritable.setUpFlow(flowWritable.getUpFlow()+flow.getUpFlow());
            flowWritable.setDownFlow(flowWritable.getDownFlow()+flow.getDownFlow());
            flowWritable.setUpCountFlow(flowWritable.getUpCountFlow()+flow.getUpCountFlow());
            flowWritable.setDownCountFlow(flowWritable.getDownCountFlow()+flow.getDownCountFlow());
        }

        context.write(key, flowWritable);
    }
}
