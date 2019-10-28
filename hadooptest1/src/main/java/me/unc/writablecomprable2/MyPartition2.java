package me.unc.writablecomprable2;

import me.unc.writablecomprable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 基于前一个需求（writablecomprable包），增加自定义分区类，分区按照省份手机号设置（手机号前三位）
 */
public class MyPartition2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        switch (text.toString().substring(0, 3)){
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
