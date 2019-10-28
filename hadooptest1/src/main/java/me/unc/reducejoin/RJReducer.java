package me.unc.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RJReducer extends Reducer<RJOrderBean, NullWritable, RJOrderBean, NullWritable> {
    @Override
    protected void reduce(RJOrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //先获取Iterable
        Iterator<NullWritable> iterator = values.iterator();
        //取第一条数据
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()) {
            //下一条数据
            iterator.next();
            //把刚取出来的pname设置进去，把bean写出去
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }
    }
}
