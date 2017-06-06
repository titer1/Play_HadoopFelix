package com.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
/**
 * 代码8-6
 * */
public class HBaseClientDeleteTable {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		//zookeeper地址
		conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk3");
		//建立连接
		Connection connection = ConnectionFactory.createConnection(conf);
		
		//表名
		String tableName = "test-hbase";

		//表管理类
		Admin admin = connection.getAdmin();
		//首先禁用表
		admin.disableTable(TableName.valueOf(tableName));
		//最后删除表
		admin.deleteTable(TableName.valueOf(tableName));
		//关闭表管理
		admin.close();
		//关闭连接
		connection.close();
	}
}
