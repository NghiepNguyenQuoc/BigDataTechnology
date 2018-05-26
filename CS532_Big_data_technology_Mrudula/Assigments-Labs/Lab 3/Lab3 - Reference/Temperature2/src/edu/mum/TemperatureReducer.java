package edu.mum;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureReducer extends Reducer<Text, PairWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (PairWritable val : values) {
            sum += val.first;
            count += val.second;
        }

        result.set(sum / count);
        context.write(key, result);
    }
}
