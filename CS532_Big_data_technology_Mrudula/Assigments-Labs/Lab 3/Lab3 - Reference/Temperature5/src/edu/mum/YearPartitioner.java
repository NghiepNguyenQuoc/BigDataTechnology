package edu.mum;

import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<ReversedIntWritable, PairWritable> {
    @Override
    public int getPartition(ReversedIntWritable key, PairWritable value, int numReduceTasks) {
        if(key.get() < 1930) {
            return 0;
        }

        return 1;
    }
}
