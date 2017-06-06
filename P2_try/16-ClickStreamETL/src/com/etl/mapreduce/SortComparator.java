package com.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{
		
	protected SortComparator() {
			super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			String[] comp1 = w1.toString().split("&");
			String[] comp2 = w2.toString().split("&");
			
			long result = 1;
			
			if(comp1 != null && comp2 != null){
				////比较sessionId
				result = comp1[0].compareTo(comp2[0]);
				////在sessionId一样的情况下比较receiveTime
				if(result == 0 && comp1.length > 1 && comp2.length > 1){
					
					long receiveTime1 = 0;
					long receiveTime2 = 0;
					
					try {
						///取得receiveTime
						receiveTime1 = Long.parseLong(comp1[1]);
						receiveTime2 = Long.parseLong(comp2[1]);
						result = receiveTime1 - receiveTime2;
						
						if(result == 0){
							//如果receiveTime相等，返回0
							return 0;
						}else{
							//如果w1的receiveTime大，返回1，否则返回-1
							return result > 0 ? 1 : -1;
						}
						
					} catch (Exception e) {
						return 1;
					}
				}
				return result > 0 ? 1 : -1;
			}
			
			return 1;
		}
	}