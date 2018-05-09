import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

	private static Schema SCHEMA_KEY;
	private static Schema SCHEMA_VALUE;

	public static class AvroMapper
			extends
			Mapper<LongWritable, Text, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		private GenericRecord recordKey = new GenericData.Record(SCHEMA_KEY);
		private GenericRecord recordValue = new GenericData.Record(SCHEMA_VALUE);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			utils.parse(value.toString());

			if (utils.isValidTemperature()) {
				recordKey.put("year", utils.getYearInt());
				recordValue.put("temperature", utils.getAirTemperature());
				recordValue.put("stationId", utils.getStationId());
				context.write(new AvroKey<GenericRecord>(recordKey),
						new AvroValue<GenericRecord>(recordValue));
			}
		}
	}

	public static class AvroReducer
			extends
			Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, AvroValue<GenericRecord>> {
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
			GenericRecord genericRecord = new GenericData.Record(SCHEMA_VALUE);
			genericRecord.put("temperature", maxTemp);
			genericRecord.put("stationId", stationID);
			context.write(key, new AvroValue<GenericRecord>(genericRecord));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
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
		String schemaFile1 = args[2];
		String schemaFile2 = args[3];

		SCHEMA_KEY = new Schema.Parser().parse(new File(schemaFile1));
		SCHEMA_VALUE = new Schema.Parser().parse(new File(schemaFile2));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);

		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(AvroValue.class);

		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(AvroValue.class);

		FileSystem fileSystem = FileSystem.get(getConf());
		Path path = new Path(args[1]);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		// Define the map output key schema
		AvroJob.setMapOutputKeySchema(job, SCHEMA_KEY);
		AvroJob.setMapOutputValueSchema(job, SCHEMA_VALUE);

		AvroJob.setOutputKeySchema(job, SCHEMA_KEY);
		AvroJob.setOutputValueSchema(job, SCHEMA_VALUE);

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroGenericStationTempYear(), args);
		System.exit(exitCode);
	}
}