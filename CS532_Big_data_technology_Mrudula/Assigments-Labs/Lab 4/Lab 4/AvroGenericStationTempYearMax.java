import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroGenericStationTempYearMax extends Configured implements Tool {

	private static Schema SCHEMA;

	public static class AvroMapper extends
			Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			utils.parse(value.toString());

			if (utils.isValidTemperature()) {
				record.put("stationId", utils.getStationId());
				record.put("MaxTemp", utils.getAirTemperature());
				record.put("year", utils.getYearInt());

				context.write(new AvroKey<GenericRecord>(record),
						NullWritable.get());
			}
		}
	}

	public static class AvroReducer
			extends
			Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {
		Set<Integer> yearSet;
		protected void setup(Context context) throws IOException ,InterruptedException {
			yearSet = new HashSet<>();
		}
		
		@Override
		protected void reduce(AvroKey<GenericRecord> key,
				Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
						
			int year = (int)key.datum().get("year");
			if(!yearSet.contains(year)){	
				context.write(key, NullWritable.get());
				yearSet.add(year);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroGenericStationTempYearMax.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);

		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);

		// Define the map output key schema
		AvroJob.setMapOutputKeySchema(job, SCHEMA);

		AvroJob.setOutputKeySchema(job, SCHEMA);
		//AvroJob.setOutputValueSchema(job, SCHEMA);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class); // Uncomment this
																// line

		// Delete output

		FileSystem fs = FileSystem.get(getConf());
		/* Check if output path (args[1])exist or not */
		if (fs.exists(new Path(args[1]))) {
			/* If exist delete the output path */
			fs.delete(new Path(args[1]), true);
		}

		//

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroGenericStationTempYearMax(), args);
		System.exit(exitCode);
		
	}
}