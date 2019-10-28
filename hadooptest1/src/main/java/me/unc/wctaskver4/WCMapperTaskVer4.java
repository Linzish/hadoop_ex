package me.unc.wctaskver4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapperTaskVer4 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    //用于返回的变量
    private IntWritable len = new IntWritable();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        len.set(words[2].length());
        context.write(len, one);
    }
}
