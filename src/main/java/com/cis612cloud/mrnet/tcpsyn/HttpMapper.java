package com.cis612cloud.mrnet.tcpsyn;

import net.ripe.hadoop.pcap.packet.Packet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by dipenpradhan on 5/1/16.
 */
class HttpMapper extends MapReduceBase implements Mapper<LongWritable, ObjectWritable, Text, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);
    private Text mapperKey = new Text();

    @Override
    public void map(LongWritable key, ObjectWritable value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        Packet packet = (Packet) value.get();
//        if (packet != null) {
//            for (Map.Entry<String, Object> e : packet.entrySet()) {
//                System.out.print(e.getKey() + " - " + e.getValue() + " |||||| ");
//            }
//        }


//        System.out.println("\n");
//        if (packet != null && packet.get(Packet.PROTOCOL) != null && packet.get(Packet.PROTOCOL).equals("TCP")) {
//            Object srcPortVal = packet.get(Packet.SRC);
//            String headerHost = (String) packet.get("header_host");
//
//            if (srcPortVal != null && headerHost != null) {
//                srcPort.set(srcPortVal + " -> " + headerHost);
//                output.collect(srcPort, ONE);
//            }
//        }

        if (packet != null && packet.get(Packet.PROTOCOL) != null && packet.get(Packet.PROTOCOL).equals("TCP")) {
            String srcIp = (String) packet.get(Packet.SRC);
            Integer srcPort = (Integer) packet.get(Packet.SRC_PORT);
            String dstIp = (String) packet.get(Packet.DST);
            Integer len = (Integer) packet.get(Packet.LEN);
//            String headerHost = (String) packet.get(DnsPacket.QNAME);

            if (srcIp != null && dstIp != null && srcPort != null
                    && srcPort == 80
                    ) {
                mapperKey.set(srcIp + " -> " + dstIp);
                output.collect(mapperKey, new LongWritable(len));
//            }
            }
        }
    }
}
