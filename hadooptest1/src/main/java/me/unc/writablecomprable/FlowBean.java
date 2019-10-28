package me.unc.writablecomprable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Writable，hadoop的序列化接口，轻量级序列化，适合服务器之间的传输
//WritableComparable序列化接口，可以用于比较bean对象
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sum;

    public FlowBean() {
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sum=" + sum +
                '}';
    }

    public void set(long upFlow, long downFlow) {
        setUpFlow(upFlow);
        setDownFlow(downFlow);
        setSum();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSum() {
        return sum;
    }

    public void setSum() {
        sum = getUpFlow() + getDownFlow();
    }

    /**
     * 序列化方法
     * @param dataOutput 框架提供的数据出口
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sum);
    }

    /**
     * 反序列化方法
     * @param dataInput 框架提供的数据来源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //！！注意！！，这里readLong的顺序要与上面序列化的顺序要一样。
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sum = dataInput.readLong();
    }

    /**
     * 比较传入的sum，
     * @param o
     * @return compare(x, y)--->(x < y) ? -1 : ((x == y) ? 0 : 1)
     */
    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(o.getSum(), this.sum);
    }
}
