import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTempStation extends Configured implements Tool {

	public static class AverageTempMapper extends
			Mapper<LongWritable, Text, StationWritable, IntWritable> {

		private StationWritable station = new StationWritable();
		private IntWritable year = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String data = value.toString();
			String stationID = data.substring(4, 10) + "-"
					+ data.substring(10, 15);
			int y = Integer.parseInt(data.substring(15, 19));

			station.setStationID(stationID);
			station.setTemperature(Integer.parseInt(data.substring(87, 92)));

			year.set(y);
			context.write(station, year);
		}

	}

	public static class AverageTempReducer extends
			Reducer<StationWritable, IntWritable, StationWritable, IntWritable> {
		@Override
		public void reduce(StationWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTempStation(), args);

		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "AverageTemp");
		job.setJarByClass(AverageTempStation.class);

		job.setMapperClass(AverageTempMapper.class);
		job.setReducerClass(AverageTempReducer.class);

		job.setOutputKeyClass(StationWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.getConfiguration().set("mapreduce.output.basename", "StationTempRecord");

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
