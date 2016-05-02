package com.cis612cloud.mrnet.analysis;

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
class AnalysisReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        int totalLen = 0;
        Set<String> domainSet = new HashSet<>();
        while (values.hasNext()) {
            String val = values.next().toString();
            String[] valSplitArr = val.split(":");
            if (valSplitArr[0].equals("l")) {
                totalLen += Integer.parseInt(valSplitArr[1]);
            } else if (valSplitArr[0].equals("d")) {
                domainSet.add(valSplitArr[1]);
            }
        }

        StringBuilder stringBuilder = new StringBuilder(totalLen+" | ");
        int i = 0;
        for (String domain : domainSet) {
            stringBuilder.append(domain);
            if (i != domainSet.size() - 1) {
                stringBuilder.append(", ");
            }
            i++;
        }
        output.collect(key, new Text(stringBuilder.toString()));
    }
}
