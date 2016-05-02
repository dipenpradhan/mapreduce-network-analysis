package com.cis612cloud.mrnet.dns;

import com.cis612cloud.mrnet.PcapRunner;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class DnsJobBuilder {

    public static Job build(Configuration conf, String... inputPaths) throws IOException {
        JobConf jobConf = new JobConf(conf, PcapRunner.class);
        jobConf.setJobName("Pcap");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setInputFormat(DnsPcapInputFormat.class);
        jobConf.setMapperClass(DnsMapper.class);
        jobConf.setReducerClass(DnsReducer.class);

        // Combine input files into splits of 100MB in size
        jobConf.setLong("mapred.max.split.size", 104857600);

        for (String path : inputPaths) {
            FileInputFormat.addInputPath(jobConf, new Path(path));
        }
        String outputPath;
//        if (inputPaths.length > 1) {
            outputPath = "pcap_output/dns";
//        } else {
//            outputPath = inputPaths[0] + "_out";
//        }
        FileUtils.deleteDirectory(new File(outputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        return new Job(jobConf);
    }

}
