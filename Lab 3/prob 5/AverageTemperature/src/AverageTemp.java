import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemp extends Configured implements Tool {

	public static class AverageTempMapper extends
			Mapper<LongWritable, Text, IntWritableComparable, PairWritable> {

		private final static IntWritable one = new IntWritable(1);
		private HashMap<IntWritableComparable, PairWritable> pairMap;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, IntWritableComparable, PairWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			pairMap = new HashMap<IntWritableComparable, PairWritable>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			IntWritableComparable year = new IntWritableComparable(
					Integer.valueOf(value.toString().substring(15, 19)));
			IntWritable airTemp = new IntWritable(Integer.valueOf(value
					.toString().substring(87, 92)));

			PairWritable pair = pairMap.get(year);
			if (pair == null) {
				pairMap.put(year, new PairWritable(airTemp, one));
			} else {
				pair.setSum(new IntWritable(pair.getSum().get() + airTemp.get()));
				pair.setCount(new IntWritable(pair.getCount().get() + 1));
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, IntWritableComparable, PairWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			for (Entry<IntWritableComparable, PairWritable> entry : pairMap
					.entrySet()) {
				context.write(entry.getKey(), entry.getValue());
			}
		}
	}

	public static class AverageTempReducer extends
			Reducer<IntWritableComparable, PairWritable, Integer, IntWritable> {

		@Override
		public void reduce(IntWritableComparable key,
				Iterable<PairWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (PairWritable val : values) {
				sum += val.getSum().get();
				count += val.getCount().get();
			}
			context.write(key.getYear(), new IntWritable(sum / count));
		}
	}

	public static class CustomPartitioner extends
			Partitioner<IntWritableComparable, PairWritable> {

		@Override
		public int getPartition(IntWritableComparable arg0, PairWritable arg1,
				int numReduceTasks) {
			if (numReduceTasks == 0)
				return 0;
			else if (arg0.year <= 1930)
				return 1 % numReduceTasks;
			else
				return 2 % numReduceTasks;
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
		job.setOutputValueClass(PairWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(CustomPartitioner.class);

		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class PairWritable implements Writable {
		IntWritable sum;
		IntWritable count;

		public PairWritable() {
		}

		public PairWritable(IntWritable sum, IntWritable count) {
			super();
			this.sum = sum;
			this.count = count;
		}

		public IntWritable getSum() {
			return sum;
		}

		public IntWritable getCount() {
			return count;
		}

		public void setSum(IntWritable year) {
			this.sum = year;
		}

		public void setCount(IntWritable count) {
			this.count = count;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			sum = new IntWritable(arg0.readInt());
			count = new IntWritable(arg0.readInt());
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeInt(sum.get());
			arg0.writeInt(count.get());
		}

		public static PairWritable read(DataInput in) throws IOException {
			PairWritable w = new PairWritable();
			w.readFields(in);
			return w;
		}
	}

	public static class IntWritableComparable implements WritableComparable {
		// Some data
		private int year;

		public IntWritableComparable() {
		}

		public IntWritableComparable(int year) {

			this.year = year;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(year);
		}

		public void readFields(DataInput in) throws IOException {
			year = in.readInt();
		}

		public int getYear() {
			return year;
		}

		public void setYear(int year) {
			this.year = year;
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			IntWritableComparable compareObject = (IntWritableComparable) o;
			int thisValue = this.year;
			int thatValue = compareObject.year;
			return (thisValue > thatValue ? -1 : (thisValue == thatValue ? 0
					: 1));
		}

		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			final int prime = 31;
			int result = 1;
			result = prime * result + year;
			return result;
		}
	}
}
