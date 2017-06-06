'''
Created on 2014-10-3

@author: Administrator
'''

from com.util.pro_env import HADOOP_PATH, PROJECT_TMP_DIR, PROJECT_LIB_DIR
import os
from com.util.hive import HiveUtils
import sys



    
#     def calculate(self, type, start_time,end_time):
#         self.prepare_normaliz(start_time, end_time)
#         self.cluster_output()
#         self.get_finalresult()
        
        
def prepare_normaliz(start_time,end_time):
    
    hql = "INSERT overwrite table user_dimension \
select t1.CustomerId,t1.avg,t2.ordercount,t3.sessioncount from \
(select CustomerId,avg(SubTotal) avg from Orders where date_part <= "" and date_part >= "" group by CustomerId) t1 join \
(select CustomerId,count(PKId) ordercount from Orders where date_part <= "" and date_part >= "" group by CustomerId) t2 on t1.CustomerId = t2.CustomerId \
join (select userId,count(sessionId) sessioncount from clickstream_log  where date_part <= "+start_time+" and date_part >= "+end_time+" group by userId) t3 on t1.CustomerId = t3.userId";
    
    HiveUtils.execute_shell(hql)
    
    hql = "INSERT overwrite table cluster_input select SubTotal, OrdersCount,SessionCount from user_dimension"
    
    HiveUtils.execute_shell(hql)
    
    hql = "INSERT overwrite table cluster_input \
select (SubTotal - avg_SubTotal)/std_SubTotal, (OrdersCount - avg_OrdersCount)/std_OrdersCount,(SessionCount - avg_SessionCount)/std_SessionCount from cluster_input \
join (select std(SubTotal) std_SubTotal,std(OrdersCount) std_OrdersCount,std(SessionCount) std_SessionCount from cluster_input) t1 on 1 = 1 \
join (select avg(SubTotal) avg_SubTotal,avg(OrdersCount) avg_OrdersCount,avg(SessionCount) avg_SessionCount from cluster_input) t2 on 1 = 1";
    
    HiveUtils.execute_shell(hql)
        
def cluster_output():
    clusterOutputPath = "/user/hadoop/clusterOutput"
    t1 = "100"
    t2 = "10"
    convergenceDelta = "0.5"
    maxIterations = "10"
    
    #执行聚类
    shell = HADOOP_PATH + "hadoop jar " + PROJECT_LIB_DIR + "usercluster.jar" + " com.mahout.UserCluster " + clusterOutputPath + " " + "/user/hive/warehouse/cluster_input " + t1 + " " + t2 + " " + convergenceDelta + " " + maxIterations 
    os.system(shell);
    
    #解析聚类结果文件并输出至本地
    resultPath = PROJECT_TMP_DIR + "result"
    shell = HADOOP_PATH + "hadoop jar " + PROJECT_LIB_DIR + "clusterout.jar" + " com.out.ClusterOutput " + clusterOutputPath + " " + resultPath
    os.system(shell);
    
    #将本地的结果文件加载Hive中
    hql = "load data loacl inpath '" + resultPath + "' overwrite into table cluster_result"
    HiveUtils.execute_shell(hql);
        
def get_finalresult():
    
    hql = "insert overwrite table final_result \
select t2.CustomerId,t1.* from \
(select clusterId,SubTotal,OrdersCount,SessionCount from cluster_result group by clusterId,SubTotal,OrdersCount,SessionCount) t1 \
join user_dimension t2 on t1.SubTotal = t2.SubTotal and t1.OrdersCount = t2.OrdersCount and t1.SessionCount = t2.SessionCount";
    HiveUtils.execute_shell(hql);
        
    


if __name__ == '__main__':
    
    start = sys.argv[1]
    end = sys.argv[2]
    
    #准备数据并做数据归一化
    prepare_normaliz(start,end)
    
    #聚类并输出
    cluster_output()
    
    #得到聚类结果
    get_finalresult()














