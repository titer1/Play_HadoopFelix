package com.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Test  extends UDF{
	public Integer evaluate(Text x) {
		// TODO Auto-generated method stub
		String xx = x.toString();
		return Integer.parseInt(xx);
	}
}
