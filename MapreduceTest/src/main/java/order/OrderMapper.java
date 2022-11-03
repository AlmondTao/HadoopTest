package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/11/3
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        context.write(new OrderBean(split[0],Double.parseDouble(split[2])),value);

    }
}
