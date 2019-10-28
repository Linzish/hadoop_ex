package me.unc.writablecomprable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 对于flow包下产生的输出文件，以总流量进行降序排序
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean flowBean = new FlowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //切分
        String[] split = value.toString().split(" ");
        //设置数值
        phone.set(split[0]);
        flowBean.set(Long.parseLong(split[1]), Long.parseLong(split[2]));
        //提交
        context.write(flowBean, phone);
    }
}
