package me.unc.sorttaskver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortTaskVerReducer extends Reducer<SortBean, IntWritable, SortBean, IntWritable> {
    @Override
    protected void reduce(SortBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            context.write(key, value);
        }
    }
}
