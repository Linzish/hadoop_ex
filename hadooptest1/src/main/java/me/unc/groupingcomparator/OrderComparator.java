package me.unc.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组在reducer之前起效
 */
public class OrderComparator extends WritableComparator {

    //通知Comparator创建OrderBean实例，转载反序列化的OrderBean数据
    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        //比较id，相同会返回0，这会分在同一组
        return oa.getOrderId().compareTo(ob.getOrderId());
    }
}
