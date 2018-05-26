package LogAnalysis;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LogAnalysis {
	static DateTimeFormatter df = DateTimeFormatter
			.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");

	public static void main(String[] args) throws Exception {

		LocalDateTime startDate = LocalDateTime.parse(
				"07/Mar/2004:00:00:00 -0800", df);
		LocalDateTime endDate = LocalDateTime.parse(
				"09/Mar/2004:00:00:00 -0800", df);

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"LogAnalysis").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		JavaPairRDD<String, Integer> count200 = lines
				.map(Log::parser)
				.filter(e -> e.getCode() == 200
						&& e.getDate().isAfter(startDate)
						&& e.getDate().isBefore(endDate))
				.mapToPair(
						c -> new Tuple2<String, Integer>(
								"From 7-Mar to 9-Mar / Total response code 200 is:",
								1)).reduceByKey((x, y) -> x + y);
		
		JavaRDD<String> ip = lines
				.map(Log::parser)
				.map(Log::getIp)
				.filter(LogAnalysis::isIP)
				.mapToPair(e -> new Tuple2<String, Integer>(e,1))
				.reduceByKey((x,y) -> x+y)
				.filter(e -> e._2 >= 10)
				.map(Tuple2::_1);
						
		deleteDirectory(new File(args[1]));
		deleteDirectory(new File(args[2]));
		count200.saveAsTextFile(args[1]);
		ip.saveAsTextFile(args[2]);
		
		

		//System.out.println("From 7-Mar to 9-Mar / Total response code 200 is" + count200);

	}

	static boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}

	static boolean isIP(String ip) {
		Pattern p = Pattern.compile("(\\d{1,3}.){3}\\d{1,3}");
		return p.matcher(ip).matches();
	}
}
