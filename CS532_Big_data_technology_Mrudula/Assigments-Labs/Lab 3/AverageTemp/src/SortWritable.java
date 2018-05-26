import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class SortWritable implements WritableComparable<SortWritable>  {
	private int val;
	
	public int compareTo(SortWritable o) {
        int thisValue = this.val;
        int thatValue = o.val;
        return (thisValue < thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
    }
	
	public int getVal() {
		return val;
	}

	public void set(int val) {
		this.val = val;
	}

	public String toString() {
        return Integer.toString(this.val);
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		val = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(val);
	}

}
