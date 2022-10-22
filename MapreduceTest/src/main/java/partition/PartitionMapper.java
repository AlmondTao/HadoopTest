package partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/10/22
 */
public class PartitionMapper extends Mapper<LongWritable, Text,LongWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitArr = value.toString().split("\t");

        context.write(new LongWritable(Integer.parseInt(splitArr[5])),value);

    }
}
