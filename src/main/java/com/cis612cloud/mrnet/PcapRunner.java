package com.cis612cloud.mrnet;

import com.cis612cloud.mrnet.analysis.AnalysisJobBuilder;
import com.cis612cloud.mrnet.dns.DnsJobBuilder;
import com.cis612cloud.mrnet.http.HttpJobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by dipenpradhan on 4/30/16.
 */
public class PcapRunner extends Configured implements Tool {

    public static int NUM_MAP_TASKS=16;
    public static int NUM_REDUCE_TASKS=16;

    public int run(String[] args) throws Exception {

        Job httpJob = HttpJobBuilder.build(getConf(), args);
        Job dnsJob = DnsJobBuilder.build(getConf(), args);
        Job analysisJob = AnalysisJobBuilder.build(getConf(), "pcap_output/http/","pcap_output/dns/");
//        Job tcpJob = TCPJobBuilder.build(getConf(), args);
//        Job synFloodAnalysisJob = SynFloodAnalysisJobBuilder.build(getConf(), "pcap_output/tcp/");

        JobControl jobControl=new JobControl("jobControl");
        jobControl.addJob(httpJob);
        jobControl.addJob(dnsJob);
        jobControl.addJob(analysisJob);
        analysisJob.addDependingJob(httpJob);
        analysisJob.addDependingJob(dnsJob);
//        jobControl.addJob(tcpJob);
//        jobControl.addJob(synFloodAnalysisJob);
//        synFloodAnalysisJob.addDependingJob(tcpJob);
        jobControl.run();

        return 1;
    }

    public PcapRunner() {
        super(new Configuration());
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PcapRunner(), args);
        System.exit(res);
    }
}


