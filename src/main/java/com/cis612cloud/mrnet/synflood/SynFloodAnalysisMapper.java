package com.cis612cloud.mrnet.synflood;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by dipenpradhan on 5/1/16.
 */
class SynFloodAnalysisMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);
    private Text mapperKey = new Text();

    @Override
    public void map(Text key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        String[] srcDestArr = key.toString().split(" -> ");
        String srcIp = srcDestArr[0].split(":")[0];

        mapperKey.set(srcIp);
        output.collect(mapperKey, new LongWritable(Long.parseLong(value.toString())));
    }
}
