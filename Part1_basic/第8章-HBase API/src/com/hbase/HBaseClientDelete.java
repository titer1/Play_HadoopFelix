package com.hbase;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

/**
 * 代码8-5
 * */
public class HBaseClientDelete {

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
		//值
		String value = "value1";
		//建立表连接
		Table table = connection.getTable(TableName.valueOf(tableName));
		//用行键来实例化Delete实例
		Delete delete = new Delete(rowkey.getBytes());
		//增加删除条件
		delete.addColumn(columnName.getBytes(), qulifier.getBytes());
		//执行删除
		table.delete(delete);
		//关闭表
		table.close();
		//关闭连接
		connection.close();
	}
}
