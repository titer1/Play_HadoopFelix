package com.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * 代码8-4
 * */
public class HBaseClientScan {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		//zookeeper地址
		conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk3 ");
		//建立连接
		Connection connection = ConnectionFactory.createConnection(conf);
		
		//表名
		String tableName = "test-hbase";
		//列族名
		String columnName = "info";
		//开始行键
		String startRow = "rk1";
		//结束行键
		String endRow = "rk5";
		//列名
		String qulifier = "c1";
		//值
		String value = "value1";

		//建立表连接
		Table table = connection.getTable(TableName.valueOf(tableName));
		//初始化Scan实例
		Scan scan = new Scan();
		//指定开始行键
		scan.setStartRow(startRow.getBytes());
		//指定结束行键
		scan.setStopRow(endRow.getBytes());
		//增加过滤条件
		scan.addColumn(columnName.getBytes(),qulifier.getBytes());
		//返回结果
		ResultScanner rs = table.getScanner(scan);
		//迭代并取出结果
		for(Result result:rs){
			String valueStr = 																Bytes.toString(result.getValue(columnName.getBytes(), 
						qulifier.getBytes()));
			System.out.println(valueStr);
		}
		//关闭表
		table.close();
		//关闭连接
		connection.close();
	}
}
