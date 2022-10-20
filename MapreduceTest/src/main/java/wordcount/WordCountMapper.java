package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/10/20
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineStr = value.toString();

        String[] wordArr = lineStr.split(",");

        for (String word : wordArr){
            context.write(new Text(word),new LongWritable(1));
        }

    }
}
