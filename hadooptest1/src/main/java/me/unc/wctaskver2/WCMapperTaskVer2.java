package me.unc.wctaskver2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapperTaskVer2 extends Mapper<LongWritable, Text, LongWritable, Text> {
    //用于返回的变量
    private LongWritable time = new LongWritable();
    private Text uid = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //LongWritable表示系统读取时按一行读取的
        //获取一行数据
        String line = value.toString();
        //按照空格切分
        String[] words = line.split("\t");
        long sTime = Long.parseLong(words[0]);
        long s = 20111230000000L;
        if (sTime >= s && sTime <= s + 10000L) {
            time.set(sTime);
            uid.set(words[1]);
            context.write(time, uid);
        }
    }
}
