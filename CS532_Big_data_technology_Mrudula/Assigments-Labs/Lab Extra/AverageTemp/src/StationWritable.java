import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;



public class StationWritable implements WritableComparable<StationWritable>  {
	private String stationID;
	private int temperature;
	
	public int compareTo(StationWritable o) {
        if (this.stationID.equals(o.stationID)) {
        	return (this.temperature < o.temperature ? 1 : (this.temperature == o.temperature ? 0 : -1));
        }
        else 
        	return this.stationID.compareTo(o.stationID);
    }
	
	
	

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stationID = in.readUTF();
		temperature = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(stationID);
		out.writeInt(temperature);
	}
	
	@Override
	public int hashCode(){
		return Objects.hash(stationID, temperature);
	}
	
	@Override
	public String toString() {
        return this.stationID + " " + Integer.toString(this.temperature);
    }

	public String getStationID() {
		return stationID;
	}

	public void setStationID(String stationID) {
		this.stationID = stationID;
	}




	public int getTemperature() {
		return temperature;
	}




	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

}
