package com.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SessionIdPartioner extends Partitioner<Text, Text>{
	
	@Override
	public int getPartition(Text key, Text value, int parts) {
		
		String sessionid = "-";
		
		if(key != null){
			sessionid = key.toString().split("&")[0];
		}
		
		
		int num = (sessionid.hashCode() & Integer.MAX_VALUE) % parts;
		
		return num;
	}		
}
