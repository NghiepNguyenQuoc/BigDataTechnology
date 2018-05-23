package cs523.kafkatest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;

import org.apache.hadoop.io.Writable;

public class TradeLine implements Writable {
	public Integer ID;
	public String TimeStamp;
	public String Quantity;
	public String Price;
	public String Total;
	public String FillType;
	public String OrderType;
	
	
	public TradeLine(Integer iD, String timeStamp, String quantity,
			String price, String total, String fillType, String orderType) {
		super();
		ID = iD;
		TimeStamp = timeStamp;
		Quantity = quantity;
		Price = price;
		Total = total;
		FillType = fillType;
		OrderType = orderType;
	}

	public TradeLine() {
		
		
	}

	@Override
	public String toString() {
		return "TradeLine [ID=" + ID + ", TimeStamp=" + TimeStamp + ", Price="
				+ Price + ", Total=" + Total + ", FillType=" + FillType
				+ ", OrderType=" + OrderType + "]";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		ID = arg0.readInt();
		TimeStamp = arg0.readUTF();
		Price = arg0.readUTF();
		Total = arg0.readUTF();
		FillType = arg0.readUTF();
		OrderType = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.write(ID);
		arg0.writeUTF(TimeStamp);
		arg0.writeUTF(Price);
		arg0.writeUTF(Total);
		arg0.writeUTF(FillType);
		arg0.writeUTF(OrderType);
	}
	
	
}
