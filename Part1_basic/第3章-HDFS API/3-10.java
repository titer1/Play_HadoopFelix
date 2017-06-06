package com.hdfsclient;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReader {
	public static void main(String[] args) {
		String uri = "hdfs://master:9000/user/hadoop/testseq";
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = null;
		
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();			
			while(reader.next(key,value)){
				System.out.printf("[%s]\t%s\n",key,value);
				position = reader.getPosition();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
