package me.unc.wctaskver2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 课程实验二
 */
public class WCTaskVerDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取一个Job实例
        Job job = Job.getInstance(new Configuration());

        //2.设置类路径（classpath）
        job.setJarByClass(WCTaskVerDriver2.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(WCMapperTaskVer2.class);
        job.setReducerClass(WCReducerTaskVer2.class);

        //4.设置Mapper输出的类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //Reducer输出的类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        //！！启用Combiner
//        job.setCombinerClass(WCReducer.class);

        //5.设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
