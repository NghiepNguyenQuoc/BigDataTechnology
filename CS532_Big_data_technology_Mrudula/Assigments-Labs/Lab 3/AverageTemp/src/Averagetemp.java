import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemp extends Configured implements Tool {

	public static class AverageTempMapper extends
			Mapper<LongWritable, Text, Text, PairWritable> {

		private Text year = new Text();
		private PairWritable pair = new PairWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			year.set(s.substring(15, 19));
			pair.setKey(Integer.parseInt(s.substring(87, 92)));
			context.write(year, pair);

		}

	}

	public static class AverageTempReducer extends
			Reducer<Text, PairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<PairWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (PairWritable val : values) {
				sum += val.getKey();
				count ++;
			}
			result.set(sum / count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemp(), args);

		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "AverageTemp");
		job.setJarByClass(AverageTemp.class);

		job.setMapperClass(AverageTempMapper.class);
		job.setReducerClass(AverageTempReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PairWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(getConf());
		/* Check if output path (args[1])exist or not */
		if (fs.exists(new Path(args[1]))) {
			/* If exist delete the output path */
			fs.delete(new Path(args[1]), true);
		}

		//

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
