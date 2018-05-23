package cs523.kafkatest;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;



public class HBaseDB
{

	public static byte[] data_cf = Bytes.toBytes("data_cf");
	
	public static byte[] id_column = Bytes.toBytes("ID");
	public static byte[] timestamp_column = Bytes.toBytes("Timestamp");
	public static byte[] quantity_column = Bytes.toBytes("Quantity");
	public static byte[] price_column = Bytes.toBytes("Price");
	public static byte[] total_column = Bytes.toBytes("Total");
	public static byte[] filltype_column = Bytes.toBytes("Filltype");
	public static byte[] ordertype_column = Bytes.toBytes("Ordertype");


	public static void createTable(Configuration config) throws IOException
	{

		
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			
			

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(App.TABLE_NAME));

			table.addFamily(new HColumnDescriptor(data_cf).setCompressionType(Algorithm.NONE));


			System.out.print("Creating table.... ");

			if (!admin.tableExists(table.getTableName()))
			{
				admin.createTable(table);
			}
			
			
			
			System.out.print(" done...");
			
			
		
		}catch(IOException ex){
			System.out.println(ex.getStackTrace());
		}
	}
	
	
	
}
