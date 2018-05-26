import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountAvroOptput extends Configured implements Tool
{

	public static class AvroWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token: value.toString().split("\\s+")) {
				word.set(token);
				context.write(word, one);
			}
		}
	}

	public static class AvroWordCountReducer extends Reducer<Text, IntWritable, AvroKey<Text>, AvroValue<Integer>>
	{

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable value : values)
			{
				sum += value.get();
			}

			context.write(new AvroKey<Text>(key), new AvroValue<Integer>(sum));
		}
	}

	public int run(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("Usage: AvroWordCount <input path> <output path>");
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(WordCountAvroOptput.class);
		job.setJobName("AvroWordCount");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AvroWordCountMapper.class);
		job.setReducerClass(AvroWordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		AvroJob.setOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setOutputValueSchema(job, Schema.create(Type.INT));
		
		
		
		//Code added to get 2a below

		FileSystem fs = FileSystem.get(getConf());
		/* Check if output path (args[1])exist or not */
		if (fs.exists(new Path(args[1]))) {
			/* If exist delete the output path */
			fs.delete(new Path(args[1]), true);
		}
		
		//

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new WordCountAvroOptput(), args);
		System.exit(res);
	}
}