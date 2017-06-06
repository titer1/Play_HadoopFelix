package com.etl.mapreduce;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.etl.utls.IpParser;

public class ClickStreamMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	//Apache日志的正则表达式
	public static final String APACHE_LOG_REGEX = "^([0-9.]+)\\s([\\w.-]+)\\s([\\w.-]+)\\s(\\[[^\\[\\]]+\\])\\s\"((?:[^\"]|\\\")+)\"\\s(\\d{3})\\s(\\d+|-)\\s\"((?:[^\"]|\\\")+)\"\\s\"((?:[^\"]|\\\")+)\"\\s\"(.+)\"\\s(\\d+|-)\\s(\\d+|-)\\s(\\d+|-)\\s(.+)\\s(\\d+|-)$";
	public static final String CANNOT_GET = "can not get";
	
	//需要获取的字段
	private String ipAddress = CANNOT_GET;
	private String uniqueId = CANNOT_GET;
	private String url = CANNOT_GET;
	private String sessionId = CANNOT_GET;
	private String sessionTimes = CANNOT_GET;
	private String areaAddress = CANNOT_GET;
	private String loaclAddress = CANNOT_GET;
	private String browserType = CANNOT_GET;
	private String operationSys = CANNOT_GET;
	private String referUrl = CANNOT_GET;
    private String receiveTime = CANNOT_GET;
	private String userId = CANNOT_GET;
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
	
		String log = value.toString();
		
		//正则解析日志
		Pattern pattern = Pattern.compile(APACHE_LOG_REGEX); 
		Matcher matcher = pattern.matcher(log);
		
		String ipStr = null;
		String receiveTimeStr = null;
		String urlStr = null;
		String referUrlStr = null;
		String userAgentStr = null;
		String cookieStr = null;
		String hostNameStr = null;
		
		
		if(matcher.find()){
			
			//根据正则表达式将日志文件断开
			ipStr = matcher.group(1);
			receiveTimeStr = matcher.group(4);
			urlStr = matcher.group(5);
			userAgentStr = matcher.group(8);
			referUrlStr = matcher.group(9);
			cookieStr = matcher.group(10);
			hostNameStr = matcher.group(14);
			
			//保存IP地址
			ipAddress = ipStr;
			
			IpParser ipParser = new IpParser();
			
			try {
				//根据IP地址得出所在区域
				areaAddress = ipParser.parse(ipStr).split(" ")[0];
				loaclAddress = ipParser.parse(ipStr).split(" ")[1];
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
			
			try {
				Date date = df.parse(receiveTimeStr);
				///将时间字符串转换为长整行
				receiveTime = Long.toString(date.getTime());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			///将url中的无效字符串丢弃
			urlStr = urlStr.substring(5);
			///重新拼装成url字符串
			url = hostNameStr + urlStr;
			
			///用户浏览器信息的正则表达式
			String userAgentRegex = "^(.+)\\s\\((.+)\\)\\s(.+)\\s\\((.+)\\)\\s(.+)\\s(.+)$";
			pattern = Pattern.compile(userAgentRegex);
			matcher = pattern.matcher(userAgentStr);
			
			///获取浏览器类型
			browserType = matcher.group(5);
			///获取操作系统类型
			operationSys = matcher.group(2).split(" ")[0];
			
			///保存上一个页面url
			referUrl = referUrlStr;
			
			///Hashmap保存cookie信息
			HashMap<String, String> cookies = new HashMap<String, String>();
			
			String[] strs = cookieStr.split(";");
			
			for (int i = 0; i < strs.length; i++) {
				String[] kv = strs[i].split("=");
				String keyStr = kv[0];
				String valStr = kv[1];
				cookies.put(keyStr, valStr);
			}
			
			///获取uuid信息
			uniqueId = cookies.get("uuid");
			
			///获取账号信息
			userId = cookies.get("userId");
			
			///如果没有获取成功，说明用户没有登录
			if(userId == null){
				userId = "un_log_in";
			}
			
			//获取seesionTimes
			sessionTimes = cookies.get("st");
			
			//拼装成sessionId
			sessionId = uniqueId + "|" + sessionTimes;
			
			//用sessionId和receiveTime组成新的key
			String mapOutKey = sessionId + "&" + receiveTime;
			//按照clickstream_log表的顺序重新组合这些字段
			String mapOutValue = ipAddress + "\t" + uniqueId + "\t" + url + "\t" + sessionId + "\t" + sessionTimes + "\t" + areaAddress + "\t" + loaclAddress + "\t" + browserType + "\t" + operationSys + "\t" + referUrl + "\t" + receiveTime + "\t" + userId;
			
			context.write(new Text(mapOutKey), new Text(mapOutValue));
			
		}else{
			return;
		}
	};
}
