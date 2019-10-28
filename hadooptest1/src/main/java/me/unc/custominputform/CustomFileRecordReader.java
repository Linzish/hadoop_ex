package me.unc.custominputform;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义一个RR，直接把这个文件读成KV对
 */
public class CustomFileRecordReader extends RecordReader<Text, BytesWritable> {
    //标记文件是否已读
    private boolean notRead = true;
    //K-V变量
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    //文件切片
    private FileSplit fs;
    //fs输入流
    private FSDataInputStream dataInputStream;

    /**
     * 初始化方法，框架会在一开始调用一次
     *
     * @param inputSplit         切块文件
     * @param taskAttemptContext context，包含任务所有信息，需要在里面get
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //转变切片类型为FileSplit（文件切片）
        fs = (FileSplit) inputSplit;
        //通过切片获取路径
        Path path = fs.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        //开流
        dataInputStream = fileSystem.open(path);
    }

    /**
     * 读取下一个key-value对
     *
     * @return 读取成功返回true，读取完成返回false
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead) {
            //具体读文件过程

            //读key
            key.set(fs.getPath().toString());

            //读value
            //创建一个与文件切片大小相同的字节流
            byte[] bytes = new byte[(int) fs.getLength()];
            //读入dataInputStream
            dataInputStream.read(bytes);
            value.set(bytes, 0, bytes.length);

            notRead = false;
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取当前key值
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前value值
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 获取当前数据读取进度
     *
     * @return 返回一个0.0-1.0的小数
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    /**
     * 用于关闭资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(dataInputStream);
    }
}
