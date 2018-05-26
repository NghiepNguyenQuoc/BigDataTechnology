package edu.mum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureCombiner extends Reducer<Text, PairWritable, Text, PairWritable> {
    private PairWritable out = new PairWritable();

    @Override
    protected void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for(PairWritable val : values) {
            sum += val.first;
            count += val.second;
        }

        out.first = sum;
        out.second = count;

        context.write(key, out);
    }
}
