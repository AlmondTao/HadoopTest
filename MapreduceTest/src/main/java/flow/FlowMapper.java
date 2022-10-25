package flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/10/25
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        String phoneNum = split[1];
        String upFlow = split[6];
        String downFlow = split[7];
        String upCountFlow = split[8];
        String downCountFlow = split[9];

        context.write(new Text(phoneNum),new FlowWritable(phoneNum,Integer.parseInt(upFlow),Integer.parseInt(downFlow),Integer.parseInt(upCountFlow),Integer.parseInt(downCountFlow)));



    }
}
