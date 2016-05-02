package com.cis612cloud.mrnet.dns;

import net.ripe.hadoop.pcap.packet.DnsPacket;
import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.List;

/**
 * Created by dipenpradhan on 5/1/16.
 */
class DnsMapper extends MapReduceBase implements Mapper<LongWritable, ObjectWritable, Text, Text> {
    private final static LongWritable ONE = new LongWritable(1);
    private Text mapperKey = new Text();

    @Override
    public void map(LongWritable key, ObjectWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Packet packet = (Packet) value.get();
//        if (packet != null) {
//            for (Map.Entry<String, Object> e : packet.entrySet()) {
//                System.out.print(e.getKey() + " - " + e.getValue() + " |||||| ");
//            }
//        }


//        System.out.println("\n");


        if (packet != null && packet.get(Packet.PROTOCOL) != null && packet.get(Packet.PROTOCOL).equals("UDP")) {
            String domain = null;
            List<String> dnsAnswer = (List<String>) packet.get(DnsPacket.ANSWER);

            if(dnsAnswer==null)return;
            for (String ans : dnsAnswer) {
                String[] splitAns = ans.split(" IN A ");
                if (splitAns.length > 1) {
                    domain = splitAns[1];
                }
            }

            String qName = (String) packet.get(DnsPacket.QNAME);
            if (domain != null && qName != null) {
                mapperKey.set(qName);
                output.collect(mapperKey, new Text(domain));
//            }
            }
        }
    }
}
