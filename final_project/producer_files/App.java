package cs523.simpleproducer;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;



public class App {
    public static void main( String[] args ) throws InterruptedException {
    	Properties props = new Properties();
    	props.put("bootstrap.servers", "localhost:9092");   
    	props.put("acks", "all");
    	props.put("retries", 0);
    	props.put("batch.size", 16384);
    	props.put("linger.ms", 1);   
    	props.put("buffer.memory", 33554432);
    	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);
    	
    	Integer id =0;
    	
    	while(true) {
    		System.out.println("sending...");
    		
    		String readString = readJson();
    		
    		producer.send(new ProducerRecord<String, String>("wordcounttopic", Integer.toString(id), readString));
    		
    		Thread.sleep(30000);
    		System.out.println("sending...done");
    		
    	}
    	
    	//producer.close();
    }
    
    private static String readJson(){
    	//inline will store the JSON data streamed in string format
		String inline = "";
	
		try
		{
			URL url = new URL("https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-BCC");
			
			//Parse URL into HttpURLConnection in order to open the connection in order to get the JSON data
			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
			//Set the request to GET or POST as per the requirements
			conn.setRequestMethod("GET");
			//Use the connect method to create the connection bridge
			conn.connect();
			//Get the response status of the Rest API
			int responsecode = conn.getResponseCode();
			System.out.println("Response code is: " +responsecode);
			
			//Iterating condition to if response code is not 200 then throw a runtime exception
			//else continue the actual process of getting the JSON data
			if(responsecode != 200)
				throw new RuntimeException("HttpResponseCode: " +responsecode);
			else
			{
				//Scanner functionality will read the JSON data from the stream
				Scanner sc = new Scanner(url.openStream());
				while(sc.hasNext())
				{
					inline+=sc.nextLine();
				}
				System.out.println("\nJSON Response in String format"); 
				System.out.println(inline);
				//Close the stream when reading the data has been finished
				sc.close();
			}
			
			
			return inline;
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return inline;
    	
    }
    
    
    
    
}
