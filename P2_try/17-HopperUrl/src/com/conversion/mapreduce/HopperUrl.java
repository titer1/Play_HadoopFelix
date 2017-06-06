package com.conversion.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HopperUrl{
	
	public static String preSessionId = "not set";
	public static int process = 0;
	static int test = 0;
	public static final String SEPARATOR = "@";
	/*
	 * 功能：正则匹配
	 * 参数：value为被匹配的内容、regex为正则
	 * 返回：匹配返回true、否则返回false
	 * */
	public static boolean regex(String value, String regex) { 
		Pattern p = Pattern.compile(regex); 
		Matcher m = p.matcher(value); 
		return m.find();
	}	
	
	/*
	 * 功能：执行漏斗功能
	 * 
	 *
	 * */
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		
		if(args.length < 4){
			System.out.println("请保证参数完整性，第一个参数为输入路径，第二个参数为输出路径，后面参数为漏斗目标url");
			return;
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		ArrayList<String> hoppers = new ArrayList<String>();
		
		for(int i = 2;i < args.length -1;i++){
			hoppers.add(args[i]);
		}
		
		String hopperId = args[args.length-1];
		configuration.set("hopper.Id", hopperId);
		
		String urls = "";
		
		for(int i = 0;i<hoppers.size();i++){
			//urls[i] = hoppers.get(i);
			urls += hoppers.get(i);
			if(i != hoppers.size()-1){
				urls += SEPARATOR;
			}
		}
		System.out.println("urls: " + urls);
		configuration.set("urls", urls);
		Job job = new Job(configuration,"hopper");
		job.setJarByClass(HopperUrl.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(NewKeyMapper.class);
		job.setReducerClass(UrlCountReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setPartitionerClass(SessionIdPartioner.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	public static class NewKeyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		/*
		 * 功能：mapper函数，取出有用的信息
		 * 参数：clickstream_log表的一行
		 * 
		 * */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] desUrlsRegex = context.getConfiguration().get("urls").split(SEPARATOR);
			/////目标url非空的判断
			if(desUrlsRegex == null){
				return;
			}
			
			if(desUrlsRegex.length == 0){
				return;
			}
			
			String[] loginfos = value.toString().split("\t");
			//String url = loginfos[2];  
			String url = loginfos[0]; 
			
			////如果该记录未访问目标地址则丢弃
			int flag = 0;
			for(int i=0;i<desUrlsRegex.length;i++){
				if(regex(url, desUrlsRegex[i])){
					break;
				}else{
					flag += 1;
				}
			}
			
			if(flag == desUrlsRegex.length){
				return;
			}
			
			String uuid = loginfos[1];   
			//String sessionId = loginfos[3];
			String sessionId = loginfos[2];
			
			try {
				//int csvp = Integer.parseInt(loginfos[5]);
				int csvp = Integer.parseInt(loginfos[3]);
				/////重新组合key，以便sort用
				String newKey = sessionId + SEPARATOR + csvp;
				String newValue = uuid + SEPARATOR + url;
				
				context.write(new Text(newKey), new Text(newValue));
			} catch (Exception e) {
				// TODO: handle exception
				return;
			}
		}
	}
	
	///自己实现分区函数，以便同一个sessionId的记录进入同一个分区
	public static class SessionIdPartioner extends Partitioner<Text, Text>{
		
		/*
		 * 功能：分区函数，自定义分发规则
		 * 参数：clickstream_log表的一行
		 * 返回：分区号
		 * */
		@Override
		public int getPartition(Text key, Text value, int parts) {
			// TODO Auto-generated method stub
			String term = "-";
			if(key != null){
				term = key.toString().split(SEPARATOR)[0];////得到sessionid
			}
			
			int num = (term.hashCode() & Integer.MAX_VALUE) % parts;//将sessionId从0到Integer的最大值散列
			
			return num;
		}		
	}
	
	///自己实现Comparator,以便能够自己实现排序规则
	public static class SortComparator extends WritableComparator{
		protected SortComparator() {
			super(Text.class, true);
		}
		
		/*
		 * 功能：比较函数，定义比较规则
		 * 参数：比较的两个值
		 * 返回：小：0，大：1
		 * */
		
		
		/////同一个sessionid按照csvp排，不同sessionId按照按照session排
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			// TODO Auto-generated method stub
			String[] comp1 = w1.toString().split(SEPARATOR);
			String[] comp2 = w2.toString().split(SEPARATOR);
			
			long result = 1;
			
			if(comp1 != null && comp2 != null){
				////比较sessionId
				result = comp1[0].compareTo(comp2[0]);
				////在sessionId一样的情况下比较csvp
				if(result == 0 && comp1.length > 1 && comp2.length > 1){
					long csvp1 = 0;
					long csvp2 = 0;
					try {
						csvp1 = Long.parseLong(comp1[1]);
						csvp2 = Long.parseLong(comp2[1]);
						result = csvp1 - csvp2;
						if(result == 0){
							return 0;
						}else{
							return result > 0 ? 1 : -1;
						}
					} catch (Exception e) {
						// TODO: handle exception
						return 1;
					}
				}
				return result > 0 ? 1 : -1;
			}
			
			return 1;
		}
	}
	
	
	public static class UrlCountReducer extends Reducer<Text, Text, NullWritable, Text>{
		
		/*
		 * 功能：reduce函数，实现漏斗功能
		 * 参数：map的输出
		 * 返回：
		 * */
		////在reduce函数中逐条读入记录，实现漏斗算法
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			String hopperId = context.getConfiguration().get("hopper.Id");
			String[] desUrlsRegex = context.getConfiguration().get("urls").split(SEPARATOR);
			String sessionId = key.toString().split(SEPARATOR)[0];
			String value = values.iterator().next().toString();
			String url = value.split(SEPARATOR)[1];
			String uuid = value.split(SEPARATOR)[0]; 
			
			
			if(preSessionId.equals("not set")){/////若是第一次执行reduce函数
						
				preSessionId = sessionId;///记录下当前sessionId
				process = 0;//初始化进度
				//if(url.equals(desUrls[0])){/////当进度为0的情况下，只需比较第一个漏斗
				if(regex(url, desUrlsRegex[0])){	
					process = 1;
					String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
					context.write(NullWritable.get(), new Text(result));
				}else{
					return;
				}
			}else{
				if(preSessionId.equals(sessionId)){////当presession = session时，说明正在进行漏斗的比较中
					if(process == desUrlsRegex.length){////判断比较是否完成
						
						////调整为一次session可以有多次会话
						process = 0;
						//if(url.equals(desUrls[0])){/////当进度为0的情况下，只需比较第一个漏斗
						if(regex(url, desUrlsRegex[0])){
							process = 1;
							String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
							context.write(NullWritable.get(), new Text(result));
						}else{
							return;
						}
						
						return;
					}else{
						//if(url.equals(desUrls[process])){///和漏斗一致
						if(regex(url, desUrlsRegex[process])){
							process += 1;////更新进度
							String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
							context.write(NullWritable.get(), new Text(result));
						}
					}
				}else{/////若果是一个新的比较
					preSessionId = sessionId;
					process = 0;
					//if(url.equals(desUrls[0])){
					if(regex(url, desUrlsRegex[0])){
						process = 1;
						String result = hopperId + "\t" + sessionId + "\t" + uuid + "\t" + process;
						context.write(NullWritable.get(), new Text(result));
					}else{
						return;
					}
				}
			}
		}
	}
}
