package me.unc.custominputform;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义FileInputFormat（切片方式）
 * 将多个小文件合并成一个SequenceFile文件（SequenceFile文件食hadoop用来存储二进制形式的key-value对的文件格式），
 * SequenceFile里面存储着多个文件，存储的形式为文件路径+名称的key，文件内容为value
 */
public class CustomFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    //设置不可切分
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    //使用自定义的RecordReader
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CustomFileRecordReader();
    }
}
