package com.hdfsclient;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListFiles {
    	public static void main(String[] args){
          String uri = "hdfs://master:9000/user";
 		Configuration conf = new Configuration();
		try {
	         FileSystem fs = FileSystem.get(URI.create(uri), conf);
	         Path path =new Path(uri);
	         FileStatus stats[]=fs.listStatus(path);
	         for(int i = 0; i < stats.length; ++i){
	        	    System.out.println(stats[i].getPath().toString());
	    	}
	         fs.close();
		}   catch (IOException e) {
			e.printStackTrace();
		}
    }
}
