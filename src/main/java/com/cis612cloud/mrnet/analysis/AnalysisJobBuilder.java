package com.cis612cloud.mrnet.analysis;

import com.cis612cloud.mrnet.PcapRunner;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import java.io.File;
import java.io.IOException;

/**
 * Created by dipenpradhan on 5/1/16.
 */
public class AnalysisJobBuilder {

    public static Job build(Configuration conf, String httpOutputPath, String dnsOutputPath) throws IOException {
        JobConf jobConf = new JobConf(conf, PcapRunner.class);
        jobConf.setJobName("Pcap");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(jobConf, new Path(httpOutputPath), KeyValueTextInputFormat.class, AnalysisHttpMapper.class);
        MultipleInputs.addInputPath(jobConf, new Path(dnsOutputPath), KeyValueTextInputFormat.class, AnalysisDnsMapper.class);
        jobConf.setReducerClass(AnalysisReducer.class);

        // Combine input files into splits of 100MB in size
        jobConf.setLong("mapred.max.split.size", 104857600);

        String outputPath;
//        if (inputPaths.length > 1) {
        outputPath = "pcap_output/analysis";
//        } else {
//            outputPath = inputPaths[0] + "_out";
//        }
        FileUtils.deleteDirectory(new File(outputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        return new Job(jobConf);
    }

}
