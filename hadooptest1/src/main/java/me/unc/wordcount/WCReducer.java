package me.unc.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //输出变量
    private IntWritable total = new IntWritable();

    /*
        values为一组<k,v>值，k值相同与参数1--key相同
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //累加和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //包装结果并输出
        total.set(sum);
        context.write(key, total);
    }
}
