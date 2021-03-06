package com.cis612cloud.mrnet.dns;

import net.ripe.hadoop.pcap.DnsPcapReader;
import net.ripe.hadoop.pcap.io.reader.PcapRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;

public class DnsPcapInputFormat extends FileInputFormat<LongWritable, ObjectWritable> {
    static final String READER_CLASS_PROPERTY = "net.ripe.hadoop.pcap.io.reader.class";

    public static final Log LOG = LogFactory.getLog(DnsPcapInputFormat.class);

    @Override
    public RecordReader<LongWritable, ObjectWritable> getRecordReader(InputSplit split, JobConf config, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit)split;
        Path path = fileSplit.getPath();
        long start = 0L;
        long length = fileSplit.getLength();
        return initPcapRecordReader(path, start, length, reporter, config);
    }

    public static PcapRecordReader initPcapRecordReader(Path path, long start, long length, Reporter reporter, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream baseStream = fs.open(path);
        DataInputStream stream = baseStream;
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(path);
        if (codec != null)
            stream = new DataInputStream(codec.createInputStream(stream));

        DnsPcapReader reader = initPcapReader(stream, conf);
        return new PcapRecordReader(reader, start, length, baseStream, stream, reporter);
    }

    public static DnsPcapReader initPcapReader(DataInputStream stream, Configuration conf) {
        try {
            Class<? extends DnsPcapReader> pcapReaderClass = conf.getClass(READER_CLASS_PROPERTY, DnsPcapReader.class, DnsPcapReader.class);
            Constructor<? extends DnsPcapReader> pcapReaderConstructor = pcapReaderClass.getConstructor(DataInputStream.class);
            return pcapReaderConstructor.newInstance(stream);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * A PCAP can only be read as a whole. There is no way to know where to
     * start reading in the middle of the file. It needs to be read from the
     * beginning to the end.
     * @see http://wiki.wireshark.org/Development/LibpcapFileFormat
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
}