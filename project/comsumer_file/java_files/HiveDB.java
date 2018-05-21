package cs523.kafkatest;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class HiveDB {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	static void InsertData(String tableName, List<TradeLine> list) throws ClassNotFoundException, SQLException{
	
	 // Register driver and create driver instance
    Class.forName(driverName);

    // get connection
    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/tradedata", "cloudera", "cloudera");

    // create statement
    Statement stmt = con.createStatement();
    
    StringBuilder sb = new StringBuilder();
    
    for (TradeLine line: list){
    
	   
	    sb.append("insert into " + tableName);
	    sb.append("values( " + line.ID + ",");
	    sb.append("'"+line.TimeStamp + "',");
	    sb.append(line.Quantity + ",");
	    sb.append( line.Price + ",");
	    sb.append(line.Total + ",");
	    sb.append("'"+line.FillType + "',");
	    sb.append("'"+line.OrderType + "'");
	    sb.append(")\n");
	    
    }
    // execute statement
   boolean sucess = stmt.execute(sb.toString());
   
    con.close();
	}
}


