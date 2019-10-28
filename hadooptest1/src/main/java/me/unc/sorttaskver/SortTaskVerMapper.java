package me.unc.sorttaskver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortTaskVerMapper extends Mapper<LongWritable, Text, SortBean, IntWritable> {
    //用于返回的变量
    private SortBean sort = new SortBean();
    private IntWritable sVar = new IntWritable();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        sort.setVar(Integer.parseInt(value.toString()));
        sVar.set(sort.getVar());
        context.write(sort, sVar);
    }
}
