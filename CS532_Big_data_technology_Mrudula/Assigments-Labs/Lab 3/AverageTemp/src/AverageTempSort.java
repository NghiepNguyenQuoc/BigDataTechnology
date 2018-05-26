import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class AverageTempSort extends Configured implements Tool {

	public static class AverageTempMapper extends
			Mapper<LongWritable, Text, SortWritable, PairWritable> {

		private SortWritable year = new SortWritable();
		//private PairWritable pair = new PairWritable();
		Map<Integer, PairWritable> map;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			map = new HashMap<>();
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int key : map.keySet()) {
				year.set(key);
				context.write(year, map.get(key));
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String s = value.toString();
			PairWritable pair;
			int year = Integer.parseInt(s.substring(15, 19));

			if (map.containsKey(year)) {
				pair = map.get(year);
			} else {
				pair = new PairWritable();
				map.put(year, pair);
			}

			pair.setKey(pair.getKey() + Integer.parseInt(s.substring(87, 92)));
			pair.setVal(pair.getVal() + 1);
		}
	}

	public static class AverageTempReducer extends
			Reducer<SortWritable, PairWritable, SortWritable, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(SortWritable key, Iterable<PairWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (PairWritable val : values) {
				sum += val.getKey();
				count += val.getVal();
			}
			result.set(sum / count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTempSort(), args);

		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "AverageTemp");
		job.setJarByClass(AverageTempSort.class);

		job.setMapperClass(AverageTempMapper.class);
		job.setReducerClass(AverageTempReducer.class);

		job.setOutputKeyClass(SortWritable.class);
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
