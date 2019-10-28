package me.unc.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 按照pid分组
 */
public class RJComparator extends WritableComparator {

    protected RJComparator() {
        super(RJOrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RJOrderBean oa = (RJOrderBean) a;
        RJOrderBean ob = (RJOrderBean) b;
        //按照pid分组
        return oa.getPid().compareTo(ob.getPid());
    }
}
