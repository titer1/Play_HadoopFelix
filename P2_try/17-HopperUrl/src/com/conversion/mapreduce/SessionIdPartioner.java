package com.conversion.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SessionIdPartioner extends Partitioner<Text, Text>{
	
	public static final String SEPARATOR = "@";
	
	
	@Override
	public int getPartition(Text key, Text value, int parts) {
		
		String sessionid = "-";
		if(key != null){
		////得到sessionid
			sessionid = key.toString().split(SEPARATOR)[0];
		}
		
		//将sessionId从0到Integer的最大值散列
		int num = (sessionid.hashCode() & Integer.MAX_VALUE) % parts;
		
		return num;
	}		
}
