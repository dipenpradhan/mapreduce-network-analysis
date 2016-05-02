package com.cis612cloud.mrnet.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by dipenpradhan on 5/1/16.
 */
class AnalysisDnsMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String[] ipArr = value.toString().split(",");

        for (String s : ipArr) {
            output.collect(new Text(ipArr[0]), new Text("d:"+key));
        }


    }
}
