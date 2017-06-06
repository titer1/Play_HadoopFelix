package com.hdfsclient;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckFileIsExist {
    public static void main(String[] args){
    	String uri = "hdfs://master:9000/user/hadoop/test";
 		Configuration conf = new Configuration();
 		
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path=new Path(url);
	        	boolean isExists=fs.exists(path);
	         	System.out.println(isExists);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
