package me.unc.wctaskver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapperTaskVer extends Mapper<LongWritable, Text, Text, IntWritable> {
    //用于返回的变量
    private Text name = new Text();
    private IntWritable len = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //LongWritable表示系统读取时按一行读取的
        //获取一行数据
        String line = value.toString();
        //按照空格切分
        String[] words = line.split("\t");
        name.set(words[1]);
        len.set(words[2].length());
        context.write(name, len);
    }
}
