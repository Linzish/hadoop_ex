package me.unc.wctaskver3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapperTaskVer3 extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    //用于返回的变量
    private LongWritable time = new LongWritable();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //LongWritable表示系统读取时按一行读取的
        //获取一行数据
        String line = value.toString();
        String[] words = line.split("\t");
        long sTime = Long.parseLong(words[0]);
        long s = 20111230000000L;
        for (;s <= 20111230235959L;s += 10000) {
            if (sTime >= s && sTime <= s + 10000) {
                time.set(s);
                context.write(time, one);
            }
        }
    }
}
