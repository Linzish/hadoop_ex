package me.unc.sorttaskver;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private int var;

    public int getVar() {
        return var;
    }

    public void setVar(int var) {
        this.var = var;
    }

    @Override
    public int compareTo(SortBean o) {
        return Integer.compare(o.var, this.var);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(var);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        var = dataInput.readInt();
    }
}
