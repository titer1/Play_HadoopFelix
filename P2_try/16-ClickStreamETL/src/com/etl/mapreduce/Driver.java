package com.etl.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		
		if(args.length != 2){
			System.out.println("参数不正确");
			return;
		}
		
		//取得输入路径，即点击流日志存放的HDFS路径
		String inputPath = args[0];
		//取得输出路径，即clickstream_log表的HDFS路径
		String outputPath = args[1];

		Job job = new Job(configuration,"clickstream_etl");
		job.setJarByClass(Driver.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(ClickStreamMapper.class);
		job.setReducerClass(ClickStreamReducer.class);
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
