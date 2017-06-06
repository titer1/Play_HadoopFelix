package com.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

//必须继承UDF类作为基类
public class RowNumUDF extends UDF{
				
	public static String signature = "-";
	public static int order = 0;
	
	public int evaluate(Text text){
		
		if(text != null){
			
			//表示分组排序的依据，列名
			String colName = text.toString();
			
			//处理第一条数据
			if(signature == "-"){
				
				//记下分组排序的字段：主键，并将rownum设为1
				signature = colName; 
				order = 1;
				
				//返回rownum
				return order;
			}else{
				//首先比对是否和上一条的主键相同
				if(signature.equals(colName)){
					
					//rownum依次加1
					order ++;
					return order;
				}else{
					
					//如果主键改变，将rownum设为1
					signature = colName;
					order = 1;
					return order;
				}
			}
		}else{
			
			//如果主键为空，则返回-1
			return -1;
		}
	}
}



