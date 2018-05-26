import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class PairWritable implements Writable {
	private int key;
	private int val;
	
	public PairWritable() {
	
	}
	public int getKey() {
		return key;
	}
	public int getVal() {
		return val;
	}
	public void setKey(int key) {
		this.key = key;
	}
	public void setVal(int val) {
		this.val = val;
	}
	public PairWritable (int key, int val){
		this.key = key;
		this.val = val;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		key = in.readInt();
		val = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(key);
		out.writeInt(val);
		
	}
	

}
