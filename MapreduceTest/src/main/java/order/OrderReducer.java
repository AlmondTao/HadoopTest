package order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/11/3
 */
public class OrderReducer extends Reducer<OrderBean, Text,Text, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int topN = 1;
        int num = 0;
        for (Text text :values){
            context.write(text,NullWritable.get());
            num++;
            if (topN == num){
                break;
            }
        }

    }
}
