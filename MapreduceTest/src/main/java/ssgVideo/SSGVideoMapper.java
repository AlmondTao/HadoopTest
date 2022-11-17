package ssgVideo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SSGVideoMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String resultStr = ETLUtils.etlData(value.toString());
        if (resultStr != null && resultStr.length() > 0){
            context.write(new Text(resultStr),NullWritable.get());
        }
    }
}
