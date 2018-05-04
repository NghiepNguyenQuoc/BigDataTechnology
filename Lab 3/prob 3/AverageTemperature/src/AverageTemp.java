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
			Mapper<LongWritable, Text, IntWritable, PairWritable> {

		private final static IntWritable one = new IntWritable(1);
		private HashMap<Integer, PairWritable> pairMap;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, IntWritable, PairWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			pairMap = new HashMap<Integer, PairWritable>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Integer year = Integer.valueOf(value.toString().substring(15, 19));
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
				Mapper<LongWritable, Text, IntWritable, PairWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			for (Entry<Integer, PairWritable> entry : pairMap.entrySet()) {
				context.write(new IntWritable(entry.getKey()), entry.getValue());
			}
		}
	}

	public static class AverageTempReducer extends
			Reducer<IntWritable, PairWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<PairWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (PairWritable val : values) {
				sum += val.getSum().get();
				count += val.getCount().get();
			}
			context.write(key, new IntWritable(sum / count));
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
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PairWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
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

		public void setSum(IntWritable sum) {
			this.sum = sum;
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
}
