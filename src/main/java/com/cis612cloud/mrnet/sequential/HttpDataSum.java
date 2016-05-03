package com.cis612cloud.mrnet.sequential; /**
 * Created by srbvsr on 5/2/16.
 */

import com.google.common.collect.Iterators;
import net.ripe.hadoop.pcap.DnsPcapReader;
import net.ripe.hadoop.pcap.HttpPcapReader;
import net.ripe.hadoop.pcap.packet.DnsPacket;
import net.ripe.hadoop.pcap.packet.HttpPacket;
import net.ripe.hadoop.pcap.packet.Packet;

import javax.management.MBeanServerConnection;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HttpDataSum {

    public static HashMap<String, String> dnsDomainToIP(String pcapFile) {

        DnsPcapReader reader = null;
        Packet[] packets = null;
        HashMap<String, String> dnsMap = new HashMap<>();   //Key Domain IP, Value Domain Name

        try {
            reader = new DnsPcapReader(new DataInputStream(new FileInputStream(pcapFile)));
        } catch (Exception io) {
            System.out.println("Unable to open pcap file");
            return null;
        }
        packets = Iterators.toArray(reader.iterator(), Packet.class);
        for (Packet pkt : packets) {
            String domain = null;

            if ((pkt.get(Packet.SRC_PORT) != null) && ((Integer) pkt.get(Packet.SRC_PORT) == 53)) {
                //DNS Response packet
                String headerHost = (String) pkt.get(DnsPacket.QNAME);
                List<String> hostAnswer = (List<String>) pkt.get(DnsPacket.ANSWER);
                if (hostAnswer != null) {
                    for (String ans : hostAnswer) {
                        String[] splitAns = ans.split(" IN A ");
                        if (splitAns.length > 1) {
                            domain = splitAns[1];
                        }
                    }
                }
                String val = dnsMap.get(headerHost);
                if (val != null) {
                    dnsMap.put(domain, val + ", " + headerHost);
                } else {
                    dnsMap.put(domain, headerHost);
                }
            }
        }
        return dnsMap;
    }

    public static HashMap<String, Integer> httpData(String pcapFile) {

        HttpPcapReader reader = null;
        Packet[] packets;
        HashMap<String, Integer> httpMap = new HashMap<>(); //Key->ClientIP:ServIP, Val:Data

        try {
            reader = new HttpPcapReader(new DataInputStream(new FileInputStream(pcapFile)));
        } catch (Exception io) {
            System.out.println("Unable to open pcap file");
            return null;
        }

        packets = Iterators.toArray(reader.iterator(), Packet.class);
        for (Packet pkt : packets) {

            if ((pkt.get(Packet.SRC_PORT) != null) && ((Integer) pkt.get(Packet.SRC_PORT) == 80)) {
                //HTTP Response packet

                String key = pkt.get(HttpPacket.SRC) + ":" + pkt.get(HttpPacket.DST);
                Integer val;
                if ((val = httpMap.get(key)) != null) {
                    httpMap.put(key, val + (Integer) pkt.get(HttpPacket.LEN));
                } else
                    httpMap.put(key, (Integer) pkt.get(HttpPacket.LEN));

            }
        }
        return httpMap;
    }

    public static void pcapAnalyse(String pcapFile) {
        HashMap<String, String> domainData = new HashMap<>();
        HashMap<String, Integer> httpMap = httpData(pcapFile);
        HashMap<String, String> dnsMap = dnsDomainToIP(pcapFile);

        Iterator itr = dnsMap.entrySet().iterator();
        while (itr.hasNext()) {

            Map.Entry<String, String> mEntry = (Map.Entry) itr.next();
            Iterator itrHttp = httpMap.entrySet().iterator();
            while (itrHttp.hasNext()) {
                Map.Entry<String, Integer> httEntry = (Map.Entry) itrHttp.next();
                String servIP = httEntry.getKey().split(":")[0];
                String domainIP = mEntry.getKey();
                if (servIP.equals(domainIP)) {
                    domainData.put(domainIP, mEntry.getValue() + " data:" + httEntry.getValue());
                }
            }
        }
        Iterator it = domainData.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> mEntry = (Map.Entry) it.next();
            System.out.println(mEntry.getKey() + " " + mEntry.getValue());
        }

    }

    public static void main(String[] args) throws Exception {

//        String pcapFile1 = "/Users/dipenpradhan/Documents/MRNetAnalysis/pcap_files/snort.log.1425892560.pcap";
//        String pcapFile2 = "/Users/dipenpradhan/Documents/MRNetAnalysis/pcap_files/snort.log.1425892797.pcap";
//        String pcapFile3 = "/Users/dipenpradhan/Documents/MRNetAnalysis/pcap_files/snort.log.1425900311.pcap";
//        String pcapFile4 = "/Users/dipenpradhan/Documents/MRNetAnalysis/pcap_files/snort.log.1425901238.pcap";
//
        MBeanServerConnection mbsc = ManagementFactory.getPlatformMBeanServer();

        OperatingSystemMXBean osMBean = ManagementFactory.newPlatformMXBeanProxy(
                mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);

        long nanoBefore = System.nanoTime();
        double cpuBefore = osMBean.getSystemLoadAverage();
        double cpuAfter = osMBean.getSystemLoadAverage();
        long nanoAfter = System.nanoTime();
        System.out.println(System.currentTimeMillis());

        for(String file:args){
            pcapAnalyse(file);
        }

//        pcapAnalyse(pcapFile2);
        //pcapAnalyse(pcapFile3);
        //pcapAnalyse(pcapFile4);
        System.out.println(System.currentTimeMillis());
        double percent;
        if (nanoAfter > nanoBefore)
            percent = ((cpuAfter - cpuBefore) * 100L) /
                    (nanoAfter - nanoBefore);
        else percent = 0;
        System.out.println("Cpu usage: " + percent + "%");
    }

}