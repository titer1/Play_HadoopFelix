package com.hdfsclient;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileWriter {
	
	private static final String[] text = {
		"两行黄鹂鸣翠柳",
		"一行白鹭上青天",
		"窗含西岭千秋雪",
		"门泊东吴万里船",
	};
	
	public static void main(String[] args) {
		String uri = "hdfs://master:9000/user/hadoop/testseq";
		Configuration conf = new Configuration();
		SequenceFile.Writer writer = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path =new Path(uri);
			IntWritable key = new IntWritable();
			Text value = new Text();
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			
			 for (int i = 0;i<100;i++){
				key.set(100-i);
				value.set(text[i%text.length]);
				writer.append(key, value);
			 }
			
		} catch (IOException e) {
			 e.printStackTrace();
		} finally {
			 IOUtils.closeStream(writer);
		}
	}
}
