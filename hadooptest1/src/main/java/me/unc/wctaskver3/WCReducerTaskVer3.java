package me.unc.wctaskver3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducerTaskVer3 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

    private IntWritable total = new IntWritable();

    @Override
    protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        total.set(sum);

        context.write(key, total);

    }
}
