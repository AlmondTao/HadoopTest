package sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/10/24
 */
public class SortReducer extends Reducer<PairWritable, NullWritable,PairWritable,NullWritable> {

    @Override
    protected void reduce(PairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for(NullWritable nullWritable : values){
            context.write(key,nullWritable);
        }
    }
}
