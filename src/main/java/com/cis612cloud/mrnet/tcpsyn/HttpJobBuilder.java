package com.cis612cloud.mrnet.tcpsyn;

import com.cis612cloud.mrnet.PcapRunner;
import net.ripe.hadoop.pcap.io.PcapInputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

import java.io.File;
import java.io.IOException;

/**
 * Created by dipenpradhan on 5/1/16.
 */
public class HttpJobBuilder {

    public static Job build(Configuration conf, String... inputPaths) throws IOException {
        JobConf jobConf = new JobConf(conf, PcapRunner.class);
        jobConf.setJobName("Pcap");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);
        jobConf.setInputFormat(PcapInputFormat.class);
        jobConf.setMapperClass(HttpMapper.class);
        jobConf.setReducerClass(HttpReducer.class);

        // Combine input files into splits of 100MB in size
        jobConf.setLong("mapred.max.split.size", 104857600);

        for (String path : inputPaths) {
            FileInputFormat.addInputPath(jobConf, new Path(path));
        }
        String outputPath;
//        if (inputPaths.length > 1) {
            outputPath = "pcap_output/http";
//        } else {
//            outputPath = inputPaths[0] + "_out";
//        }
        FileUtils.deleteDirectory(new File(outputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        return new Job(jobConf);
    }

}
