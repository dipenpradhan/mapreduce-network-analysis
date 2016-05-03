package com.cis612cloud.mrnet.tcp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by dipenpradhan on 5/1/16.
 */
class TCPReducer extends MapReduceBase implements Reducer<Text, Text, Text, LongWritable> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        boolean syn = false, ack = false;
        while (values.hasNext()) {
            String truthVals = values.next().toString();

            if (truthVals.charAt(0) == 'T') {
                syn = true;
            }
            if (truthVals.charAt(2) == 'T' || truthVals.charAt(2) == 'T') {
                ack = true;
            }

        }
        if (syn && !ack) {
            output.collect(key, new LongWritable(1));
        }
    }
}
