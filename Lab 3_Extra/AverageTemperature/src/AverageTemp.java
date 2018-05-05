import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemp extends Configured implements Tool {

	public static class AverageTempMapper extends
			Mapper<LongWritable, Text, IntWritableComparable, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String stationValue = value.toString().substring(4, 9) + "-"
					+ value.toString().substring(10, 14);

			IntWritableComparable stationID = new IntWritableComparable(
					stationValue, Integer.valueOf(value.toString().substring(
							87, 92)));

			IntWritable year = new IntWritable(Integer.valueOf(value.toString()
					.substring(15, 19)));
			context.write(stationID, year);
		}
	}

	public static class AverageTempReducer
			extends
			Reducer<IntWritableComparable, IntWritable, IntWritableComparable, IntWritable> {

		@Override
		public void reduce(IntWritableComparable key,
				Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new AverageTemp(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Average Temperature");
		job.setJarByClass(AverageTemp.class);
		job.setMapperClass(AverageTempMapper.class);
		job.setReducerClass(AverageTempReducer.class);
		job.setOutputKeyClass(IntWritableComparable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(CustomOutputFormat.class);
		// job.setNumReduceTasks(1);

		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class IntWritableComparable implements Comparator,
			WritableComparable {
		// Some data
		private String stationID;
		private int temperature;

		public IntWritableComparable() {
		}

		public IntWritableComparable(String stationID, int temperature) {
			this.stationID = stationID;
			this.temperature = temperature;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(stationID);
			out.writeInt(temperature);
		}

		public void readFields(DataInput in) throws IOException {
			stationID = in.readUTF();
			temperature = in.readInt();
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			IntWritableComparable compareObject = (IntWritableComparable) o;
			int result = stationID.compareTo(compareObject.stationID);
			if (result == 0)
				return compareObject.temperature - temperature;
			return result;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((stationID == null) ? 0 : stationID.hashCode());
			result = prime * result + temperature;
			return result;
		}

		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			IntWritableComparable object1 = (IntWritableComparable) o1;
			IntWritableComparable object2 = (IntWritableComparable) o2;
			int result = object1.stationID.compareTo(object2.stationID);
			if (result == 0)
				return object2.temperature - object1.temperature;
			return result;
		}

		@Override
		public String toString() {
			return stationID + "\t" + temperature;
		}
	}

	static class CustomOutputFormat<K, V> extends TextOutputFormat<K, V> {
		@Override
		public Path getDefaultWorkFile(TaskAttemptContext context,
				String extension) throws IOException {
			Path baseOutputPath = FileOutputFormat.getOutputPath(context);
			final Path outputFilePath = new Path(baseOutputPath,
					"StationTempRecord");
			return outputFilePath;
		}
	}
}
