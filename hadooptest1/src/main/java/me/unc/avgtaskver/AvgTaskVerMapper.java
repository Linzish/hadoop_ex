package me.unc.avgtaskver;

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
public class AvgTaskVerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //用于返回的变量
    private Text name = new Text();
    private IntWritable avg = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //LongWritable表示系统读取时按一行读取的
        //获取一行数据
        int sAvg = 0;
        String line = value.toString();
        //按照空格切分
        String[] words = line.split("\t");
        //遍历数组，把单词变成（word, 1）的形式输入到框架中
        this.name.set(words[0]);
        for (String word : words) {
            if (word.matches("[0-9]+")) {
                sAvg += Integer.parseInt(word);
            }
        }
        sAvg = sAvg / words.length-2;
        avg.set(sAvg);
        context.write(name, avg);
    }
}
