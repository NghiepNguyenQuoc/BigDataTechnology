package cs523.kafkatest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

//spark 
//spark-submit --class "cs523.kafkatest.App" kafkatest.jar
//spark-submit --class "cs523.kafkatest.App" '/home/cloudera/Downloads/v0.1/kafkatest/target/kafkatest-0.0.1-SNAPSHOT-jar-with-dependencies.jar' 

public class App {
	static int MAX_SELL_ID= 0;
	static int MAX_BUY_ID= 0;
	public static final String TABLE_NAME = "TradeData";
	
    public static void main( String[] args ) throws IOException {
    	
    	Map<String, String> kafkaParams = new HashMap<String, String>();
    	kafkaParams.put("bootstrap.servers", "localhost:9092");
    	
    	Set<String> topics = new HashSet<String>();
    	topics.add("wordcounttopic");
    	
    	SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaTest");
    	
    	JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

    	JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(10));
    	JavaPairInputDStream<String, String> javaPairInputDStream = KafkaUtils.createDirectStream(
    			javaStreamingContext,
    			String.class,
    			String.class,
    			StringDecoder.class,
    			StringDecoder.class,
    			kafkaParams,
    			topics);
    	
    	//JavaDStream<String> lines = javaPairInputDStream.map(t -> t._2());
    	//List<TradeLine> list = readJson(lines.toString());						
    														
    	//List<TradeLine> newRecords = list.stream().filter(tl -> (tl.OrderType == "SELL" && tl.ID> MAX_SELL_ID)
    							//|| (tl.OrderType == "BUY" && tl.ID> MAX_BUY_ID)).collect(Collectors.toList());
    	
    	// HBASE
    	Configuration configuration = HBaseConfiguration.create();
    	HBaseDB.createTable(configuration);
    	TableName tableName = TableName.valueOf(TABLE_NAME);
    	
    	JavaHBaseContext javaHBaseContext = new JavaHBaseContext(javaSparkContext, configuration);
    	javaHBaseContext.streamBulkPut(javaPairInputDStream.toJavaDStream()
    			.flatMap(x -> readJson(x._2)),tableName,
								                x -> {
								                		return App.processLine(x);
								                	
								                }
                					);
    	
    
    	
    	javaStreamingContext.start();
    	javaStreamingContext.awaitTermination();
    }
    
    private static List<TradeLine> readJson(String line){
    	List<TradeLine> list = new LinkedList<>();
	
		try
		{
			
			//JSONParser reads the data from string object and break each data into key value pairs
			JSONParser parse = new JSONParser();
			//Type caste the parsed json data in json object
			JSONObject jobj = (JSONObject)parse.parse(line);
			//Store the JSON object in JSON array as objects (For level 1 array element i.e Results)
			JSONArray jsonarr_1 = (JSONArray) jobj.get("result");
			//Get data for Results array
			for(int i=0;i<jsonarr_1.size();i++)
			{
				//Store the JSON objects in an array
				//Get the index of the JSON object and print the values as per the index
				JSONObject jsonobj_1 = (JSONObject)jsonarr_1.get(i);
				

				TradeLine tempLine = new TradeLine();
				
				tempLine.ID = Integer.valueOf(jsonobj_1.get("Id").toString());
				tempLine.TimeStamp = jsonobj_1.get("TimeStamp").toString();
				tempLine.Quantity = jsonobj_1.get("Quantity").toString();
				tempLine.Price = jsonobj_1.get("Price").toString();
				tempLine.Total= jsonobj_1.get("Total").toString();
				tempLine.FillType= jsonobj_1.get("FillType").toString();
				tempLine.OrderType= jsonobj_1.get("OrderType").toString();
				
				
				list.add(tempLine);
				
				System.out.println(tempLine);
				
			}
			
			return list;
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return list;
    	
    }
    
    static Put processLine(TradeLine line) {
    	
    	Put put = new Put(Bytes.toBytes(line.ID));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.id_column, Bytes.toBytes(line.ID));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.timestamp_column, Bytes.toBytes(line.TimeStamp));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.quantity_column, Bytes.toBytes(line.Quantity));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.price_column, Bytes.toBytes(line.Price));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.total_column, Bytes.toBytes(line.Total));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.filltype_column, Bytes.toBytes(line.FillType));
    	put.addColumn(HBaseDB.data_cf, HBaseDB.ordertype_column, Bytes.toBytes(line.OrderType));
    	
    	return put;
    }
    
   
}
