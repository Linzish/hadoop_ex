package me.unc.custominputform;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 自定义inputformat
 */
public class CustomDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //Mapper和Reducer不修改可以不些，有默认实现
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(CustomDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //设置自定义FileInputFormat
        job.setInputFormatClass(CustomFileInputFormat.class);
        //合并成SequenceFile文件，生成文件用于SequenceFile模式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
