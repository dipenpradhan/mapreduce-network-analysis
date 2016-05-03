package com.cis612cloud.mrnet.tcp;

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
class TCPMapper extends MapReduceBase implements Mapper<LongWritable, ObjectWritable, Text, Text> {
    private final static LongWritable ONE = new LongWritable(1);
    private Text mapperKey = new Text();

    @Override
    public void map(LongWritable key, ObjectWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Packet packet = (Packet) value.get();

        if (packet != null && packet.get(Packet.PROTOCOL) != null && packet.get(Packet.PROTOCOL).equals("TCP")) {
            Boolean tcpSyn = (Boolean) packet.get(Packet.TCP_FLAG_SYN);
            Boolean tcpAck = (Boolean) packet.get(Packet.TCP_FLAG_ACK);
            Boolean tcpRst = (Boolean) packet.get(Packet.TCP_FLAG_RST);
            Boolean tcpPsh = (Boolean) packet.get(Packet.TCP_FLAG_PSH);
            String srcIp = (String) packet.get(Packet.SRC);
            String dstIp = (String) packet.get(Packet.DST);
            Integer srcPort = (Integer) packet.get(Packet.SRC_PORT);
            Integer dstPort = (Integer) packet.get(Packet.DST_PORT);

            StringBuilder valBuilder = new StringBuilder();
            valBuilder.append(tcpSyn != null && tcpSyn ? "T" : "F");
            valBuilder.append(tcpAck != null && tcpAck ? "T" : "F");
            valBuilder.append(tcpRst != null && tcpRst ? "T" : "F");
            valBuilder.append(tcpPsh != null && tcpPsh ? "T" : "F");

            if (srcIp != null && dstIp != null && srcPort != null
                    && dstPort == 80
                    ) {
                mapperKey.set(srcIp + ":" + srcPort + " -> " + dstIp + ":" + dstPort);
                output.collect(mapperKey, new Text(valBuilder.toString()));
//            }
            }
        }
    }
}
