package me.unc.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<>();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //从context中获取缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        //获取文件路径
        String path = cacheFiles[0].getPath();

        //开流
        // （win本地流ver）（bufferedReader.readLine()）
//        BufferedReader bufferedReader = new BufferedReader((new InputStreamReader(new FileInputStream(path))));

        //HDFS版本（可能存在乱码问题，需要自己设计包装类）（！！可能存在问题）
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream bufferedReader = fileSystem.open(new Path(path));

        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readUTF())) {
            //缓存进map
            String[] fields = line.split("\t");
            pMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        //获取pname
        String pname = pMap.get(split[1]);
        if (pname == null) {
            pname = "NULL";
        }
        //替换
        k.set(split[0] + "\t" + pname + "\t" + split[2]);
        context.write(k, NullWritable.get());
    }
}
