package com.cis612cloud.mrnet.dns;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by dipenpradhan on 5/1/16.
 */
class DnsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        Set<String> ips = new HashSet<>();

        while (values.hasNext()) {
            ips.add(values.next().toString());
        }

        StringBuilder stringBuilder = new StringBuilder();
        int i = 0;
        for (String ip : ips) {
            stringBuilder.append(ip);
            if (i != ips.size() - 1) {
                stringBuilder.append(", ");

            }
            i++;
        }
        output.collect(key, new Text(stringBuilder.toString()));
    }
}
