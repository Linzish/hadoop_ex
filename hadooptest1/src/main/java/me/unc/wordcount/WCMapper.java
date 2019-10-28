package me.unc.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Mapper参数：<输入键类型，输入值类型，输出键类型，输出值类型>
hadoop序列化类型对比
Boolean-->BooleanWritable、Byte-->ByteWritable、Int-->IntWritable
Float-->FloatWritable、Long-->LongWritable、Double-->DoubleWritable
String-->Text、Map-->MapWritable、Array-->ArrayWritable
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //用于返回的变量
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //LongWritable表示系统读取时按一行读取的
        //获取一行数据
        String line = value.toString();
        //按照空格切分
        String[] words = line.split(" ");
        //遍历数组，把单词变成（word, 1）的形式输入到框架中
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, one);
        }
    }
}
