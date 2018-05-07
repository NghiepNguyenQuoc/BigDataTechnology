import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Iterables;

public class AvroGenericStationTempYear extends Configured implements Tool {

	private static Schema SCHEMA;

	public static class AvroMapper
			extends
			Mapper<LongWritable, Text, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord record = new GenericData.Record(SCHEMA);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			utils.parse(value.toString());

			if (utils.isValidTemperature()) {
				record.put("year", utils.getYearInt());
				record.put("temperature", utils.getAirTemperature());
				record.put("stationId", utils.getStationId());
				context.write(new AvroKey<GenericRecord>(record),
						new AvroValue<GenericRecord>(record));
			}
		}
	}

	public static class AvroReducer
			extends
			Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
		@Override
		protected void reduce(AvroKey<GenericRecord> key,
				Iterable<AvroValue<GenericRecord>> values, Context context)
				throws IOException, InterruptedException {
			double maxTemp = (double) Iterables.get(values, 0).datum()
					.get("temperature");
			String stationID = (String) Iterables.get(values, 0).datum()
					.get("stationId");
			for (AvroValue<GenericRecord> avroValue : values) {
				double temp = (double) avroValue.datum().get("temperature");
				if (temp > maxTemp) {
					maxTemp = temp;
					stationID = (String) avroValue.datum().get("stationId");
				}
			}
			key.datum().put("temperature", maxTemp);
			key.datum().put("stationId", stationID);
			context.write(key, NullWritable.get());
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
		job.setJarByClass(AvroGenericStationTempYear.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);

		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(AvroValue.class);
		
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		// Define the map output key schema
		AvroJob.setMapOutputKeySchema(job, SCHEMA);
		AvroJob.setMapOutputValueSchema(job, SCHEMA);

		AvroJob.setOutputKeySchema(job, SCHEMA);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroGenericStationTempYear(), args);
		System.exit(exitCode);
	}
}