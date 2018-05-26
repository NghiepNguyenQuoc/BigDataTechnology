package edu.mum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReversedIntWritable implements WritableComparable<ReversedIntWritable> {

    private int value;

    @Override
    public int compareTo(ReversedIntWritable o) {
        int thisValue = this.value;
        int thatValue = o.value;
        return Integer.compare(thatValue, thisValue);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readInt();
    }

    public void set(int value) {
        this.value = value;
    }

    public int get() {
       return value;
    }

    public String toString() {
        return Integer.toString(this.value);
    }
//    static {
//        WritableComparator.define(ReversedIntWritable.class, new ReversedIntWritable.Comparator());
//    }
//
//    public static class Comparator extends WritableComparator {
//        public Comparator() {
//            super(IntWritable.class);
//        }
//
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            int thisValue = readInt(b1, s1);
//            int thatValue = readInt(b2, s2);
//            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
//        }
//    }
}
