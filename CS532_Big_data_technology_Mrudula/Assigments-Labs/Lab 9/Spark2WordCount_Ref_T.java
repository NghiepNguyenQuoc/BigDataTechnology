import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class Spark2WordCount
{
    static final int THRESHOLD = 3;

	public static void main(String[] args) throws Exception
	{
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<Character, Integer> counts = lines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<>(w, 1))
					.reduceByKey((x, y) -> x + y)
                    .filter(t -> t._2() < THRESHOLD)
                    .flatMap((FlatMapFunction<Tuple2<String, Integer>, Tuple2<Character, Integer>>) t -> t._1.chars()
                            .mapToObj(c -> new Tuple2<>((char) c, t._2))
                            .collect(Collectors.toList()))
                    .mapToPair(t -> t)
                    .filter(t -> (t._1 >= 'a' && t._1 <= 'z') || (t._1 >= 'A' && t._1 <= 'Z'))
                    .reduceByKey((x, y) -> x + y);

		// Save the word count back out to a text file, causing evaluation
		counts.saveAsTextFile(args[1]);

		sc.close();
	}
}
