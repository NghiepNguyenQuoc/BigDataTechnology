package edu.mum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, PairWritable> {
    private Text year = new Text();
    private PairWritable out = new PairWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();

        year.set(data.substring(15, 19));
        out.first =  Integer.parseInt(data.substring(87, 92));
        out.second = 1;

        context.write(year, out);
    }
}
