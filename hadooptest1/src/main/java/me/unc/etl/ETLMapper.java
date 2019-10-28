package me.unc.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        //TODO 自定义过滤策略
        //过滤长度小于11的数据
        if (split.length > 11) {
            context.write(value, NullWritable.get());
            //设置计数器，合法加一
            context.getCounter("ETL", "TRUE").increment(1);
        } else {
            //不合法加一
            context.getCounter("ETL", "FALSE").increment(1);
        }
    }
}
