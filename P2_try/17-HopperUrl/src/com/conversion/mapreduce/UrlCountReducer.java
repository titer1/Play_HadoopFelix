package com.conversion.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlCountReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	//表示前一条记录的SessionId
	public static String preSessionId = "not set";
	//表示漏斗的步骤，如1为漏斗的第一步
	public static int process = 0;
	public static final String SEPARATOR = "@";
	
	public static boolean regex(String value, String regex) { 
		Pattern p = Pattern.compile(regex); 
		Matcher m = p.matcher(value); 
		return m.find();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		//从Context对象中取得漏斗的Id
		String hopperId = context.getConfiguration().get("hopper.Id");
		//从Context对象中取得表示漏斗的url正则表达式
		String[] desUrls = context.getConfiguration().get("urls").split("|");
		//取得SessionId
		String sessionId = key.toString().split(SEPARATOR)[0];
		String value = values.iterator().next().toString();
		//取得url
		String url = value.split(SEPARATOR)[1];
		//取得uuid
		String uuid = value.split(SEPARATOR)[0]; 
		
		
		if(preSessionId.equals("not set")){/////若是第一次执行reduce函数
					
			preSessionId = sessionId;///记录下当前sessionId
			process = 0;//初始化进度
			
			if(regex(url, desUrls[0])){	
				process = 1;
				String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
				context.write(NullWritable.get(), new Text(result));
			}else{
				return;
			}
			
		}else{
			////当presession = session时，说明正在进行漏斗的比较中
			if(preSessionId.equals(sessionId)){////当presession = session时，说明正在进行漏斗的比较中
				////一个漏斗比较完成
				if(process == desUrls.length){
					//开始新的漏斗比较
					process = 0;
					//当进度为0的情况下，只需比较第一个漏斗
					if(regex(url, desUrls[0])){
						process = 1;
						//输出的格式为：漏斗Id + SessionId + uuid + 漏斗的进度
						String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
						context.write(NullWritable.get(), new Text(result));
					}else{
						return;
					}
					return;
				}else{
					///符合漏斗模型的url
					if(regex(url, desUrls[process])){
						process ++;////更新进度
						//输出的格式为：漏斗Id + SessionId + uuid + 漏斗的进度
						String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
						context.write(NullWritable.get(), new Text(result));
					}
				}
			}else{/////若果是一个新SessionId
				preSessionId = sessionId;
				process = 0;
				if(regex(url, desUrls[0])){
					process = 1;
					//输出的格式为：漏斗Id + SessionId + uuid + 漏斗的进度
					String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
					context.write(NullWritable.get(), new Text(result));
				}else{
					return;
				}
			}
		}
	}
}