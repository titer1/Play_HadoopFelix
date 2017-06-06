package com.conversion.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NewKeyMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public static final String SEPARATOR = "@";
	
	//用正则的方式判断是否相等
	public static boolean regex(String value, String regex) { 
		Pattern p = Pattern.compile(regex); 
		Matcher m = p.matcher(value); 
		return m.find();
	}	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		////从Context对象中取得表示漏斗的url正则表达式
		String[] desUrlsRegex = context.getConfiguration().get("urls").split("|");
		
		//如果表示漏斗的url为空，则返回
		if(desUrlsRegex == null){
			return;
		}
		
		//表示x表中的一行，按照分隔符切开
		String[] loginfos = value.toString().split("\t");
		
		//获取url
		String url = loginfos[0]; 
		
		////如果该记录未访问目标地址则丢弃
		int flag = 0;
		for(int i = 0;i < desUrlsRegex.length; i++){
			if(regex(url, desUrlsRegex[i])){
				break;
			}else{
				flag += 1;
			}
		}
		
		if(flag == desUrlsRegex.length){
			return;
		}
		
		//获取用户的唯一id
		String uuid = loginfos[1];
		//获取SessionId
		String sessionId = loginfos[2];
		
		try {
			//获取csvp
			int csvp = Integer.parseInt(loginfos[3]);
			//将SessionId和csvp组合成为新的key
			String newKey = sessionId + SEPARATOR + csvp;
			//剩下的部分作为新的value
			String newValue = uuid + SEPARATOR + url;
			//输出
			context.write(new Text(newKey), new Text(newValue));
		} catch (Exception e) {
			return;
		}
	}
}
