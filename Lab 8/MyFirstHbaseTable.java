import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable {

	private static final String TABLE_NAME = "user";
	private static final String CF_PERSONAL_DETAIL = "Personal Details";
	private static final String CF_PROFESSIONAL_DETAIL = "Professional Details";
	private static final String CELL_NAME = "Name";
	private static final String CELL_CITY = "City";
	private static final String CELL_DESIGNATION = "Designation";
	private static final String CELL_SALARY = "Salary";

	public static void main(String... args) throws IOException {

		Configuration config = HBaseConfiguration.create();
		 initTable(config);
		 insertData(config, "1", "John", "Boston", "Manager", "150000");
		 insertData(config, "2", "Mary", "New York", "Sr. Engineer",
		 "130000");
		 insertData(config, "3", "Bob", "Fremont", "Jr. Engineer", "90000");

		// update John's salary to 160000
		udpateSalary(config, "1", "160000");
		getNumberOfRow(config);
	}

	public static void initTable(Configuration config) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_PERSONAL_DETAIL)
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_PROFESSIONAL_DETAIL));
			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");
		}
	}

	@SuppressWarnings("deprecation")
	public static void insertData(Configuration config, String rowid,
			String name, String city, String designation, String salary)
			throws IOException {
		// Instantiating HTable class
		HTable hTable = new HTable(config, TABLE_NAME);

		// Instantiating Put class
		// accepts a row name.
		Put p = new Put(Bytes.toBytes(rowid));

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.add(Bytes.toBytes(CF_PERSONAL_DETAIL), Bytes.toBytes(CELL_NAME),
				Bytes.toBytes(name));

		p.add(Bytes.toBytes(CF_PERSONAL_DETAIL), Bytes.toBytes(CELL_CITY),
				Bytes.toBytes(city));

		p.add(Bytes.toBytes(CF_PROFESSIONAL_DETAIL),
				Bytes.toBytes(CELL_DESIGNATION), Bytes.toBytes(designation));

		p.add(Bytes.toBytes(CF_PROFESSIONAL_DETAIL),
				Bytes.toBytes(CELL_SALARY), Bytes.toBytes(salary));

		// Saving the put Instance to the HTable.
		hTable.put(p);
		System.out.println(rowid + "data inserted");

		// closing HTable
		hTable.close();
	}

	@SuppressWarnings("deprecation")
	public static void udpateSalary(Configuration config, String rowId,
			String salary) throws IOException {
		// Instantiating HTable class
		HTable hTable = new HTable(config, TABLE_NAME);

		// Instantiating Put class
		// accepts a row name
		Put p = new Put(Bytes.toBytes(rowId));

		// Updating a cell value
		p.add(Bytes.toBytes(CF_PROFESSIONAL_DETAIL), Bytes.toBytes(CELL_SALARY),
				Bytes.toBytes(salary));

		// Saving the put Instance to the HTable.
		hTable.put(p);
		System.out.println("data Updated");

		// closing HTable
		hTable.close();
	}

	@SuppressWarnings("resource")
	public static void getNumberOfRow(Configuration config) throws IOException {
		// Instantiating HTable class
		HTable hTable = new HTable(config, TABLE_NAME);

		// Instantiating the Scan class
		Scan scan = new Scan();

		// Scanning the required columns
		scan.addColumn(Bytes.toBytes(CF_PERSONAL_DETAIL),
				Bytes.toBytes(CELL_NAME));
		scan.addColumn(Bytes.toBytes(CF_PERSONAL_DETAIL),
				Bytes.toBytes(CELL_CITY));
		scan.addColumn(Bytes.toBytes(CF_PROFESSIONAL_DETAIL),
				Bytes.toBytes(CELL_DESIGNATION));
		scan.addColumn(Bytes.toBytes(CF_PROFESSIONAL_DETAIL),
				Bytes.toBytes(CELL_SALARY));

		// Getting the scan result
		ResultScanner scanner = hTable.getScanner(scan);

		int numberOfRow = 0;
		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner
				.next()) {
			numberOfRow++;
		}

		System.out.println("Number of row in " + TABLE_NAME + " table: "
				+ numberOfRow);
		// closing the scanner
		scanner.close();
	}
}
