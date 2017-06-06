package com.out;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
 
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
 
public class ClusterOutput {
    public static void main(String[] args) {
        try {
            
        	//Mahout的输出文件,需要被解析
        	String clusterOutputPath = args[0];
        	//解析后的聚类结果文件，将输出至本地磁盘
        	String resultPath = args[1];
        	
        	BufferedWriter bw;
            Configuration conf = new Configuration();
            conf.set("fs.default.name", "hdfs://192.168.190.200:9000");
            FileSystem fs = FileSystem.get(conf);
 
            SequenceFile.Reader reader = null;
            
            reader = new SequenceFile.Reader(fs, new Path(clusterOutputPath + "/clusteredPoints/part-m-00000"), conf);
 
            bw = new BufferedWriter(new FileWriter(new File(resultPath)));
            
            //key为聚簇中心id
            IntWritable key = new IntWritable();
            WeightedVectorWritable value = new WeightedVectorWritable();
            
            while (reader.next(key, value)) {
                //得到向量
            	RandomAccessSparseVector vector = (RandomAccessSparseVector) value.getVector();
                
                String vectorvalue = "";
                
                //将向量各个维度拼接成一行，用\t分隔
                for(int i = 0;i < vector.size();i++){
                	
                	if(i == vector.size() - 1){
                		vectorvalue += vector.get(i);
                	}else{
                		vectorvalue += vector.get(i) + "\t";
                	}
                	
                }
                
                //在向量前加上该向量属于的聚簇中心id
                bw.write(key.toString() + "\t" + vectorvalue + "\n");
            }
            
            bw.flush();
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
} 