package me.unc.avgtaskver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgTaskVerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /*
        values为一组<k,v>值，k值相同与参数1--key相同
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            context.write(key, value);
        }
    }
}
