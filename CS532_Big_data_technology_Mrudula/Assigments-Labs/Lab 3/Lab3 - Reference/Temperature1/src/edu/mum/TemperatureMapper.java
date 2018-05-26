package edu.mum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text year = new Text();
    private IntWritable temperature = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();

        year.set(data.substring(15, 19));
        temperature.set(Integer.parseInt(data.substring(87, 92)));

        context.write(year, temperature);
    }
}
