package edu.mum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TemperatureMapper extends Mapper<LongWritable, Text, MyKeyWritable, IntWritable> {
    private MyKeyWritable myKey = new MyKeyWritable();
    private IntWritable yearOut = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();

        String stationId = data.substring(4, 10) + "-" + data.substring(10, 15);
        int year = Integer.parseInt(data.substring(15, 19));

        myKey.stationId = stationId;
        myKey.temperature = Integer.parseInt(data.substring(87, 92));

        yearOut.set(year);
        context.write(myKey, yearOut);
    }
}
