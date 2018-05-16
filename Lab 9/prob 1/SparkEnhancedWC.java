package cs523.SparkEnhancedWC;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Char;
import scala.Tuple2;

public class SparkEnhancedWC {
	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> counts_b = lines
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);

		// Save the word count back out to a text file, causing evaluation
		JavaRDD<String> counts_c = getAllWordByThreshold(counts_b, 60);
		JavaPairRDD<String, Integer> counts_d = charFrequency(counts_c, 60);

		counts_b.coalesce(1).saveAsTextFile(args[1] + "_b");
		counts_c.coalesce(1).saveAsTextFile(args[1] + "_c");
		counts_d.coalesce(1).saveAsTextFile(args[1] + "_d");

		sc.close();
	}

	public static JavaRDD<String> getAllWordByThreshold(
			JavaPairRDD<String, Integer> counts, int threshold) {
		return counts.filter(t -> t._2 >= threshold).map(t -> t._1);
	}

	public static JavaPairRDD<String, Integer> charFrequency(
			JavaRDD<String> counts, int threshold) {
		return counts
				.flatMap(key -> Arrays.asList(key.split("")))
				.mapToPair(
						ch -> new Tuple2<String, Integer>(ch.toLowerCase(), 1))
				.reduceByKey((x, y) -> x + y);
	}
}
