package com.etl.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClickStreamReducer extends Reducer<Text, Text, NullWritable, Text>{

	///��ʾǰһ��sessionId
	public static String preSessionId = "-";
	static int csvp = 0;
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		
		
		
		String sessionId = key.toString().split("&")[0];		
		
		//����ǵ�һ�����
		if(preSessionId.equals("-")){
			csvp = 1;
			preSessionId = sessionId;
		}else{
			//�����ǰһ��sessionId��ͬ��˵����ͬһ��session
			if(preSessionId.equals(sessionId)){
				//�ۼ�csvp
				csvp++;
			//���ͬ��˵�����µ�session������preSessionId��csvp
			}else{
				preSessionId = sessionId;
				csvp = 1;
			}
		}
		
		//����clickstream_log�ĸ�ʽ��ĩβ����csvp
		String reduceOutValue = values.iterator().next().toString() + "\t" + csvp;
		context.write(NullWritable.get(), new Text(reduceOutValue));
	};
}
