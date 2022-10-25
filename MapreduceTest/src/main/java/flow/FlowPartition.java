package flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author TaoQingYang
 * @date 2022/10/25
 */
public class FlowPartition extends Partitioner<Text,FlowWritable> {


    @Override
    public int getPartition(Text text, FlowWritable flowWritable, int i) {
        String phoneNum = text.toString();
        if (phoneNum.startsWith("135")){
            return 0;
        }else if(phoneNum.startsWith("136")){
            return 1;
        }else if(phoneNum.startsWith("137")){
            return 2;
        }
        return 3;
    }
}
