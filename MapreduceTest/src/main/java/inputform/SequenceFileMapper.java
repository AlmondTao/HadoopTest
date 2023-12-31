package inputform;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/11/2
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        context.write(new Text(name),value);

    }
}
