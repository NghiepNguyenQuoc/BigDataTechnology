package edu.mum;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReducer extends Reducer<MyKeyWritable, IntWritable, MyKeyWritable, IntWritable> {
    @Override
    public void reduce(MyKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for(IntWritable val : values) {
            context.write(key, val);
        }
    }
}
