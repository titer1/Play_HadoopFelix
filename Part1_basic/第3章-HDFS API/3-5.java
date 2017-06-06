package com.hdfsclient;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteFile {
     public static void main(String[] args){
    	String uri = "hdfs://master:9000/user/hadoop/test";
 		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path delef=new Path("hdfs://master:9000/user/hadoop");
	         boolean isDeleted=fs.delete(delef,false);
	         //是否递归删除文件夹及文件夹下的文件
	         //boolean isDeleted=fs.delete(delef,true);
	         System.out.println(isDeleted);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
