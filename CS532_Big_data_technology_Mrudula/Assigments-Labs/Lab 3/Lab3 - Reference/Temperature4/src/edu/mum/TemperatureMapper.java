package edu.mum;

import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TemperatureMapper extends Mapper<LongWritable, Text, ReversedIntWritable, PairWritable> {
    private ReversedIntWritable key = new ReversedIntWritable();
    Map<Integer, PairWritable> yearMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        yearMap = new HashMap<>();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int year : yearMap.keySet()) {
            key.set(year);
            context.write(key, yearMap.get(year));
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        PairWritable pair;

        int year = Integer.parseInt(data.substring(15, 19));

        if(yearMap.containsKey(year)) {
            pair = yearMap.get(year);
        } else {
            pair = new PairWritable(0, 0);
            yearMap.put(year, pair);
        }

        pair.first += Integer.parseInt(data.substring(87, 92));
        pair.second += 1;
    }
}
