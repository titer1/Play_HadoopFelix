package com.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


/**
 * 代码8.1
 * 
 * */
public class HBaseClient {

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

		//表管理类
		Admin admin = connection.getAdmin();
		//定义表名
		HTableDescriptor tableDescriptor = new 
				HTableDescriptor(TableName.valueOf(tableName));
		admin.createTable(tableDescriptor);
		//定义表结构
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnName);
		admin.addColumn(TableName.valueOf(tableName), columnDescriptor);
		admin.close();
		connection.close();

	}
}
