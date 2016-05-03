package com.cis612cloud.mrnet.synflood;

import com.cis612cloud.mrnet.PcapRunner;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;

import java.io.File;
import java.io.IOException;

/**
 * Created by dipenpradhan on 5/1/16.
 */
public class SynFloodAnalysisJobBuilder {

    public static Job build(Configuration conf, String... inputPaths) throws IOException {
        JobConf jobConf = new JobConf(conf, PcapRunner.class);
        jobConf.setJobName("SYN Flood");
        jobConf.setNumMapTasks(PcapRunner.NUM_MAP_TASKS);
        jobConf.setNumReduceTasks(PcapRunner.NUM_REDUCE_TASKS);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);
        jobConf.setInputFormat(KeyValueTextInputFormat.class);
        jobConf.setMapperClass(SynFloodAnalysisMapper.class);
        jobConf.setReducerClass(SynFloodAnalysisReducer.class);

        // Combine input files into splits of 100MB in size
        jobConf.setLong("mapred.max.split.size", 104857600);

        for (String path : inputPaths) {
            FileInputFormat.addInputPath(jobConf, new Path(path));
        }
        String outputPath;
        outputPath = "pcap_output/synflood";
        FileUtils.deleteDirectory(new File(outputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        return new Job(jobConf);
    }

}
