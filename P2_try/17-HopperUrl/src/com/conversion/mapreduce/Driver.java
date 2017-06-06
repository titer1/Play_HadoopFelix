package com.conversion.mapreduce;

import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.conversion.mapreduce.NewKeyMapper;
import com.conversion.mapreduce.SessionIdPartioner;
import com.conversion.mapreduce.SortComparator;
import com.conversion.mapreduce.UrlCountReducer;

public class Driver {
	
	public static final String SEPARATOR = "@";
	
	public static void main(String[] args) throws Exception {
			
			Configuration configuration = new Configuration();
			
			if(args.length <= 2){
				System.out.println("请保证参数完整性，第一个参数为输入路径，第二个参数为输出路径，后面参数为漏斗目标url");
				return;
			}
			
			//取得输入路径，即x表的HDFS路径
			String inputPath = args[0];
			//取得输出路径，即y表的HDFS路径
			String outputPath = args[1];
			
			//保存表示漏斗的url的正则表达式
			ArrayList<String> hoppers = new ArrayList<String>();
			for(int i = 2;i < args.length -1;i++){
				hoppers.add(args[i]);
			}
			
			//取得漏斗id，为传入参数的最后一个
			String hopperId = args[args.length-1];
			//将漏斗id保存到configuration对象中，供所有Map Task和Reduce Task使用
			configuration.set("hopper.Id", hopperId);
			
			String urls = "";
			for(int i = 0;i<hoppers.size();i++){
				//urls[i] = hoppers.get(i);
				urls += hoppers.get(i);
				if(i != hoppers.size()-1){
					urls += "|";
				}
			}
			//将漏斗id保存到configuration对象中，供所有Map Task和Reduce Task使用
			configuration.set("urls", urls);
			
			
			Job job = new Job(configuration,"sales");
			job.setJarByClass(Driver.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.setMapperClass(NewKeyMapper.class);
			job.setReducerClass(UrlCountReducer.class);
			//手动设置Reducer的个数，该值可根据集群计算能力酌情考虑
			job.setNumReduceTasks(4);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setPartitionerClass(SessionIdPartioner.class);
			job.setSortComparatorClass(SortComparator.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
