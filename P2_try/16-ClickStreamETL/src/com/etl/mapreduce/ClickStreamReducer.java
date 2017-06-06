package com.etl.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClickStreamReducer extends Reducer<Text, Text, NullWritable, Text>{

	///表示前一个sessionId
	public String preSessionId = "-";
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		
		int csvp = 0;
		
		String sessionId = key.toString().split("&")[0];		
		
		//如果是第一条数据
		if(preSessionId.equals("-")){
			csvp = 1;
		}else{
			//如果与前一个sessionId相同，说明是同一个session
			if(preSessionId.equals(sessionId)){
				//累加csvp
				csvp++;
			//如果不同，说明是新的session，重置preSessionId和csvp
			}else{
				preSessionId = sessionId;
				csvp = 1;
			}
		}
		
		//按照clickstream_log的格式在末尾加上csvp
		String reduceOutValue = values.iterator().next().toString() + "\t" + csvp;
		context.write(NullWritable.get(), new Text(reduceOutValue));
	};
}
