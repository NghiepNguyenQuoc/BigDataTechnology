package edu.mum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Temperature extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new Temperature(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "Temperature");
        job.setJarByClass(Temperature.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setCombinerClass(TemperatureCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, removeAndSetOutput(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    private Path removeAndSetOutput(String outputDir) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        Path path = new Path(outputDir);
        fs.delete(path, true);
        return path;
    }

}
