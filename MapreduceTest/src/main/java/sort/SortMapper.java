package sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/10/24
 */
public class SortMapper extends Mapper<LongWritable, Text,PairWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");


        context.write(new PairWritable(split[0],Integer.parseInt(split[1])),NullWritable.get());
    }
}
