package me.unc.customoutputform;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出，把文件输出指定目录指定文件名
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    //TODO 定义my名字，还有write方法
    private FSDataOutputStream my;
    private FSDataOutputStream other;

    /**
     * 手动调用初始化方法
     * @param job 包含job的所有参数
     */
    public void init(TaskAttemptContext job) throws IOException {
        String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        my = fs.create(new Path(outDir + "/my.log"));
        other = fs.create(new Path(outDir + "/other.log"));
    }

    /**
     * 写数据，将KV写出，每对KV调用一次
     * @param longWritable
     * @param text
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {
        String out = text.toString() + "\t";
        if (out.contains("my")) {
            my.write(out.getBytes());
        } else {
            other.write(out.getBytes());
        }
    }

    /**
     * 关闭资源
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(my);
        IOUtils.closeStream(other);
    }
}
