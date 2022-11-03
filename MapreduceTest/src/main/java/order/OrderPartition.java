package order;

import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.Text;

/**
 * @author TaoQingYang
 * @date 2022/11/3
 */
public class OrderPartition extends Partitioner<OrderBean, Text> {
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        return (orderBean.getOrderId().hashCode() & 2147483647) % i;
    }
}
