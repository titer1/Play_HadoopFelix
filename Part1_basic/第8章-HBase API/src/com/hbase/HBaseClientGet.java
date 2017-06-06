package com.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 代码8-3
 * */
public class HBaseClientGet {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		//zookeeper地址
		conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk3");
		//建立连接
		Connection connection = ConnectionFactory.createConnection(conf);
		
		//表名
		String tableName = "test-hbase";
		//列族名
		String columnName = "info";
		//行键
		String rowkey = "rk1";
		//列名
		String qulifier = "c1";

		//建立表连接
		Table table = connection.getTable(TableName.valueOf(tableName));
		//用行键实例化Get
		Get get = new Get(rowkey.getBytes());
		//增加列族名和列名条件
		get.addColumn(columnName.getBytes(), qulifier.getBytes());
		//执行Get，返回结果
		Result result = table.get(get);
		//取出结果
		String valueStr = Bytes.toString(result.getValue(columnName.getBytes(), 
				qulifier.getBytes()));
		System.out.println(valueStr);
		//关闭表
		table.close();
		//关闭连接
		connection.close();

	}
}
