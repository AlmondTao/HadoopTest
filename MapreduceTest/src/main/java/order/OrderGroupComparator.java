package order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author TaoQingYang
 * @date 2022/11/3
 */
public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean orderBeanA = (OrderBean)a;
        OrderBean orderBeanB = (OrderBean)b;
        return orderBeanA.getOrderId().compareTo((orderBeanB).getOrderId());
    }
}
