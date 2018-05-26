package cs523.SparkWC;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Spark2WordCountjdk8 {

	public static void main(String[] args) throws Exception
	{
		//Threshold 
		
		int threshold = 100;
		
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

				
		// Load our input data
			
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		
		// Calculate word count
		JavaPairRDD<String, Integer> counts = lines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y);
		
		JavaRDD<String> ft = counts
					.filter(e -> e._2 >= threshold)
					.map(e -> e._1);
		
		JavaPairRDD<Character, Integer> count_char = ft
				.flatMap(f->f.chars().mapToObj(m -> (char)m).collect(Collectors.toList()))
				.mapToPair(l-> new Tuple2<Character, Integer>(l, 1))
				.reduceByKey((x, y) -> x + y);
					
				
		// Save the word count back out to a text file, causing evaluation
		
			counts.coalesce(1).saveAsTextFile(args[1]+"_c");
			ft.coalesce(1).saveAsTextFile(args[1]+"_d");
			count_char.coalesce(1).saveAsTextFile(args[1]+"_e");

		sc.close();
	}
}
