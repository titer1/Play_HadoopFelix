# -*- coding:UTF-8 -*-
'''
Created on 2014-3-21

@author: liwei
'''

from com.util.pro_env import * 
from xml.etree import ElementTree as ET

#todo 仅仅是保证了编译通过
def CalSalesCount(self):

#     def calculate(self):
#         #CalTest.calculateTest(self)
#         try:
#             #解析配置文件，获取运行参数
#             self.resolveTheConf()
#         except Exception:
#             print "运行参数有误，中断执行"
#             return
#         
#         #提取所需要的数据
#         self.extractTheData();
#         
#         #通过MapReduce作业进行初步统计
#         self.countTheSales();
#         
#         #将上一步输出的中间结果加载到Hive中并进行汇总得到最终结果
#         self.getTheResult();
    
    def resolveTheConf(start,end,hid):
        confFile = PROJECT_CONF_DIR + "Conversion.xml"
        xmlTree = ET.parse(confFile)
        eles = xmlTree.findall('./pras')
        
        pras = eles[0]
        self.urls = []
        
        for pra in pras.getchildren():
            #print pra.tag,':',pra.text
            if  pra.tag == 'url':
                self.urls.append(pra.text.strip())
            if pra.tag == 'id':
                self.id = pra.text.strip()
            if pra.tag == 'start':
                self.start = pra.text.strip()
            if pra.tag == 'end':
                self.end = pra.text.strip()
        
        if len(self.urls) == 0 or self.id == None or self.id == '' or self.start == None or self.start == '' or self.end == None or self.end == '':
            raise Exception('sales')
        
        for url in self.urls:
            if url == None or url == '':
                raise Exception('sales')
    
    def extractTheData():
        hql = "insert into table sales_input partition (ds='"+ self.start + "-" + self.end + "') \
select url,uuid,sessionid,csvp from clickstream_log where ds >= " + self.start +" and ds <= " + self.end
        print hql
        #HiveUtils.executebyshell(hql, False);
    
    def countTheSales():
        #MapReduce作业的输入路径，为sales_input表的HDFS地址
        input = "/user/warehouse/sales_input/ds=" + self.start + "-" + self.end
        #MapReduce作业的输出路径，可以任意指定
        self.output = "/user/temp"
        urlstr = "";
        
        #将表示漏斗的正则表达式拼装成一个字段串，作为参数传给MapReduce作业
        for i in range(len(self.urls)):
            if(i == len(self.urls) -1):
                urlstr += self.urls[i]
            else:
                urlstr += self.urls[i] + " "
        
        #拼装成shell命令
        shell = HADOOP_PATH + "hadoop jar " + PROJECT_LIB_DIR + "sales.jar " + "com.sales.mapreduce.Driver " + input + " " + self.output + " " + urlstr
        
        #执行命令
        #os.system(shell)
        print shell
    
    def getTheResult():
        #最终结果表的分区
        ds = self.start + "-" + self.end
        
        #删除作业成功的标志性文件
        shell = HADOOP_PATH + "hadoop dfs -rm " + self.output + "/_SUCCESS"
        #os.system(shell)
        print shell
        
        #删除作业的日志文件
        shell = HADOOP_PATH + "hadoop dfs -rmr " + self.output + "/_logs"
        #os.system(shell)
        print shell
        
        #将临时结果加载到中间结果表
        hql = "load data inpath '" + self.output + "' overwrite into table sales_middle_result partition (ds = " + ds +")" 
        #HiveUtils.executebyshell(hql, False)
        print hql
        
        #对中间结果进行汇总并写入最后结果表
        hql = "insert into table sales_result partition (ds='"+ self.start + "-" + self.end + "') \
select hid,process,count(process),count(distinct(uuid)),process from sales_middle_result where ds = " + ds + " group by hid,process"
        print hql
        #HiveUtils.executebyshell(hql, False)
        
if __name__ == '__main__': 
    aa = CalSalesCount()
    #CalTest.printStatic()
    #aa.resolveTheConf()
    #aa.extractTheData()
    #aa.countTheSales();
    #aa.getTheResult();
    #aa.hh()
    
        