package partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author TaoQingYang
 * @date 2022/10/22
 */
public class MyPartitioner extends Partitioner <LongWritable, Text> {

    @Override
    public int getPartition(LongWritable longWritable, Text text, int i) {
        long num = longWritable.get();

        if (num%2==1){
            return 1;
        }else{
            return 0;
        }


    }
}
