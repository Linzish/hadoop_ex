package me.unc.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Map Join适用于一张十分小，一张很大的场景
 * 小文件一般保持15m以下
 * Hive默认25m为小表，以上为大表
 * 本实验模拟Hive的处理原理，以后有类似情景使用Hive
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MJMapper.class);
        //不开设ReduceTask
        job.setNumReduceTasks(0);

        //TODO 定义详细的文件全路径
        //把一张表缓存在内存，缓存的表无需在InputPaths里
        //win --> file:///d:/xxx/xxx.txt / hdfs --> ...
        job.addCacheFile(URI.create(args[3]));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
