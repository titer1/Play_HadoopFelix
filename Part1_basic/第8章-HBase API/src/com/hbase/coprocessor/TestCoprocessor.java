package com.hbase.coprocessor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
/*
 *代码8-8 
 * 
 */

public class TestCoprocessor extends BaseRegionObserver {

	@Override
     public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, 
             final Put put, final WALEdit edit, final Durability durability) 
     throws IOException { 
         Configuration conf = new Configuration(); 
         Connection connection = ConnectionFactory.createConnection(conf);
         //索引表
         Table table = connection.getTable(TableName.valueOf("index_table"));
         //取出要插入的数据
         List<Cell> cells = put.get("cf".getBytes(), "info".getBytes()); 
      
         Iterator<Cell> kvItor = cells.iterator();   
         
         while (kvItor.hasNext()) { 
    
        	 Cell tmp = kvItor.next();          
        	 //用值作为行键
             Put indexPut = new Put(tmp.getValue()); 
             indexPut.add("cf".getBytes(), tmp.getRow(), 
                    Bytes.toBytes(System.currentTimeMillis()));
             //插入索引表
             table.put(indexPut);
         } 
         
         table.close(); 
         connection.close();
     } 
} 
