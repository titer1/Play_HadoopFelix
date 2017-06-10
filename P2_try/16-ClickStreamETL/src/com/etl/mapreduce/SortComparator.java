package com.etl.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{
		
	protected SortComparator() {
			super(Text.class, true);
		}
		
		@Override
		public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2) {
			
			String[] comp1 = w1.toString().split("&");
			String[] comp2 = w2.toString().split("&");
			
			long result = 1;
			
			if(comp1 != null && comp2 != null){
				////�Ƚ�sessionId
				result = comp1[0].compareTo(comp2[0]);
				////��sessionIdһ�������±Ƚ�receiveTime
				if(result == 0 && comp1.length > 1 && comp2.length > 1){
					
					long receiveTime1 = 0;
					long receiveTime2 = 0;
					
					try {
						///ȡ��receiveTime
						receiveTime1 = Long.parseLong(comp1[1]);
						receiveTime2 = Long.parseLong(comp2[1]);
						result = receiveTime1 - receiveTime2;
						
						if(result == 0){
							//���receiveTime��ȣ�����0
							return 0;
						}else{
							//���w1��receiveTime�󣬷���1�����򷵻�-1
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