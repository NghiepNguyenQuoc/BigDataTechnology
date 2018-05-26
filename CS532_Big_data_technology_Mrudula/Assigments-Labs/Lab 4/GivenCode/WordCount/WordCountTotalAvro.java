import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class WordCountTotalAvro extends Configured implements Tool
{

	public static class AvroWordCountMapper extends Mapper<LongWritable, Text, AvroKey<String>, AvroValue<Integer>>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token: value.toString().split("\\s+")) {
				context.write(new AvroKey<String>(token), new AvroValue<Integer>(1));
			}
		}
	}

	public static class AvroWordCountReducer extends Reducer<//complete the parameters here...>
	{

		public void reduce(AvroKey<String> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (AvroValue<Integer> value : values)
			{
				sum += value.datum();
			}

			context.write(key, new AvroValue<Integer>(sum));
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
		job.setJarByClass(WordCountTotalAvro.class);
		job.setJobName("AvroWordCount");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		

		job.setMapperClass(AvroWordCountMapper.class);
		job.setReducerClass(AvroWordCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		AvroJob.setMapOutputKeySchema(job, Schema.create(Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Schema.create(Type.INT));		
		
		//Set the output key and value schema for AvroJob here

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new WordCountTotalAvro(), args);
		System.exit(res);
	}
}