package com.conversion.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{
		
	protected SortComparator() {
			super(Text.class, true);
		}
		
		public static final String SEPARATOR = "@";
		
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			String[] comp1 = w1.toString().split(SEPARATOR);
			String[] comp2 = w2.toString().split(SEPARATOR);
			
			long result = 1;
			
			if(comp1 != null && comp2 != null){
				////比较sessionId
				result = comp1[0].compareTo(comp2[0]);
				////在sessionId一样的情况下比较csvp
				if(result == 0 && comp1.length > 1 && comp2.length > 1){
					
					long csvp1 = 0;
					long csvp2 = 0;
					
					try {
						//取得csvp
						csvp1 = Long.parseLong(comp1[1]);
						csvp2 = Long.parseLong(comp2[1]);
						result = csvp1 - csvp2;
						
						if(result == 0){
							//如果csvp相等，返回0
							return 0;
						}else{
							//如果w1的csvp大，返回1，否则返回-1
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