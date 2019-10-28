package me.unc.custompartition;

import me.unc.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<Text, FlowBean> {
    /**
     * 返回分区号。分区号用于设置该分区给哪个ReduceTask
     * @param text 输入
     * @param flowBean 输出包装bean
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();

        switch (phone.substring(0, 3)){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
