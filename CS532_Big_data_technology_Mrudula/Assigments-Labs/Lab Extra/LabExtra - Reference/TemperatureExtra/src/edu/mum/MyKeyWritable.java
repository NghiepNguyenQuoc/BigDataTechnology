package edu.mum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKeyWritable implements WritableComparable<MyKeyWritable> {

    public String stationId;
    public int temperature;

    @Override
    public int compareTo(MyKeyWritable o) {
        if(this.stationId.equals(o.stationId)) {
            return Integer.compare(o.temperature, this.temperature);
        }
        return this.stationId.compareTo(o.stationId);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.stationId);
        out.writeInt(this.temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stationId = in.readUTF();
        this.temperature = in.readInt();
    }

    public String toString() {
        return this.stationId + " " + Integer.toString(this.temperature);
    }

    @Override
    public int hashCode() {
        int result = 13;

        result += 21 * this.stationId.hashCode();
        result += 21 * this.temperature;

        return result;
    }
}