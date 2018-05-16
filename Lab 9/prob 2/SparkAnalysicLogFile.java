package cs523.AnalysicLogFile;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Char;
import scala.Tuple2;

public class SparkAnalysicLogFile {
	public static final int NUM_FIELDS = 9;
	static DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

	static LocalDateTime startDate = LocalDateTime.parse(
			"07/Mar/2004:00:00:00 -0800", formatter);
	static LocalDateTime endDate = LocalDateTime.parse(
			"12/Mar/2004:00:00:00 -0800", formatter);

	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"Analysic Log File").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		JavaPairRDD<String, Integer> countError = lines
				.map(entry -> LogEnTry.parser(entry))
				.filter(t -> t.getCode() == 404
						&& t.getDate().isAfter(startDate)
						&& t.getDate().isBefore(endDate))
				.mapToPair(
						s -> new Tuple2<String, Integer>(
								"Number of 404 error in 07/Mar - 12/Mar, 2004: ",
								1)).reduceByKey((x, y) -> x + y);

		JavaRDD<String> ip = lines
				.map(entry -> LogEnTry.parser(entry))
				.map(i -> i.getIp())
				.mapToPair(e -> new Tuple2<String, Integer>(e,1))
				.reduceByKey((x,y) -> x+y)
				.filter(e -> e._2 >= 20)
				.map(Tuple2::_1);
		
		countError.saveAsTextFile(args[1]+"_count_error");
		ip.saveAsTextFile(args[1]+"_list_ip");
		sc.close();
	}
}
