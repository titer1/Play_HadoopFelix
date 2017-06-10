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
	
	//Apache��־��������ʽ
	public static final String APACHE_LOG_REGEX = "^([0-9.]+)\\s([\\w.-]+)\\s([\\w.-]+)\\s(\\[[^\\[\\]]+\\])\\s\"((?:[^\"]|\\\")+)\"\\s(\\d{3})\\s(\\d+|-)\\s\"((?:[^\"]|\\\")+)\"\\s\"((?:[^\"]|\\\")+)\"\\s\"(.+)\"\\s(\\d+|-)\\s(\\d+|-)\\s(\\d+|-)\\s(.+)\\s(\\d+|-)$";
	public static final String CANNOT_GET = "can not get";
	
	//��Ҫ��ȡ���ֶ�
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
		
		//���������־
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
			
			//���������ʽ����־�ļ��Ͽ�
			ipStr = matcher.group(1);
			receiveTimeStr = matcher.group(4);
			urlStr = matcher.group(5);
			userAgentStr = matcher.group(9);
			referUrlStr = matcher.group(8);//sequence
			cookieStr = matcher.group(10);
			hostNameStr = matcher.group(14);
			
			//����IP��ַ
			ipAddress = ipStr;
			
			IpParser ipParser = new IpParser();
			
			try {
				//���IP��ַ�ó���������
				areaAddress = ipParser.parse(ipStr).split(" ")[0];
				loaclAddress = ipParser.parse(ipStr).split(" ")[1];
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.US);
			receiveTimeStr=receiveTimeStr.replace("[","").replace("]","");
			try {
				Date date = df.parse(receiveTimeStr);
				///��ʱ���ַ�ת��Ϊ������
				receiveTime = Long.toString(date.getTime());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			///��url�е���Ч�ַ���
			urlStr = urlStr.substring(5);
			///����ƴװ��url�ַ�
			url = hostNameStr +"/"+ urlStr;//fix
			
			///�û��������Ϣ��������ʽ
			String userAgentRegex = "^(.+)\\s\\((.+)\\)\\s(.+)\\s\\((.+)\\)\\s(.+)\\s(.+)$";
			pattern = Pattern.compile(userAgentRegex);
			
			/*
			matcher = pattern.matcher(userAgentStr);
			
			///��ȡ���������
			browserType = matcher.group(5);
			///��ȡ����ϵͳ����
			operationSys = matcher.group(2).split(" ")[0];
			*/
			
			///������һ��ҳ��url
			referUrl = referUrlStr;
			
			///Hashmap����cookie��Ϣ
			HashMap<String, String> cookies = new HashMap<String, String>();
			
			String[] strs = cookieStr.split(";");
			
			for (int i = 0; i < strs.length; i++) {
				String[] kv = strs[i].split("=");
				String keyStr = kv[0];
				String valStr = kv[1];
				cookies.put(keyStr, valStr);
			}
			
			///��ȡuuid��Ϣ
			uniqueId = cookies.get("uuid");
			
			///��ȡ�˺���Ϣ
			userId = cookies.get("userId");
			
			///���û�л�ȡ�ɹ���˵���û�û�е�¼
			if(userId == null){
				userId = "un_log_in";
			}
			
			//��ȡseesionTimes
			sessionTimes = cookies.get("st");
			
			//ƴװ��sessionId
			sessionId = uniqueId + "|" + sessionTimes;
			
			//��sessionId��receiveTime����µ�key
			String mapOutKey = sessionId + "&" + receiveTime;
			//����clickstream_log���˳�����������Щ�ֶ�
			String mapOutValue = ipAddress + "\t" + uniqueId + "\t" + url + "\t" + sessionId + "\t" + sessionTimes + "\t" + areaAddress + "\t" + loaclAddress + "\t" + browserType + "\t" + operationSys + "\t" + referUrl + "\t" + receiveTime + "\t" + userId;
			
			context.write(new Text(mapOutKey), new Text(mapOutValue));
			
		}else{
			return;
		}
	};
}
