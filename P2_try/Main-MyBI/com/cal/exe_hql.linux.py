# -*- coding:UTF-8 -*-
'''
Created on 2014-8-23

@author: Administrator
'''

import os
import sys 
'''
sys.path.append('/media/sf_Hadoop_felixP2_try/Main-MyBI/com/cal/Basic.py')
'''
sys.path.append(os.path.abspath('../../'))


from com.cal.Basic import BasicCalculate
from com.util.pro_env import *
from xml.etree import ElementTree as ET
from com.util.hive import HiveUtils



#question1 parameter
#question2 xml not found ,hivejob.xml ?
#quesiton3  BasicCalculate from  com.cal.Basic 

#class CalQuery(BasicCalculate):   
class CalQuery():   
    def __init__(self):
        '''
        Constructor
        ''' 
    def resolveTheConf(self):
        #获得配置文件名
        #66: create xml as format
        confFile = PROJECT_CONF_DIR + "Query.xml"
        
        
        #解析配置文件
        xmlTree = ET.parse(confFile)
        #获得pras元素
        eles = xmlTree.findall('./pras')
        
        pras = eles[0]
        
        #遍历pras的子元素，获得所需参数
        for pra in pras.getchildren():
            #获得hql标签的值
            if pra.tag == 'hql':
                self.hql = pra.text.strip()
            #获得output标签的值     
            if pra.tag == 'output':
                self.output = pra.text.strip()
            #获得filepath标签的值
            if pra.tag == 'filepath':
                self.filepath = pra.text.strip()
        
        #检查参数有效性，无效则抛出异常       
        if len(self.hql) == 0 or self.hql == "" or self.hql == None:
            raise Exception('参数有误，终止运行')
        
        #检查参数有效性，无效则抛出异常 
        if len(self.output) == 0 or self.output == "" or self.output == None:
            raise Exception(' 参数有误，终止运行')
        
        #检查参数有效性，无效则抛出异常 
        if self.output == 'true' and (len(self.filepath) == 0 or self.filepath == "" or self.filepath == None):
            raise Exception('参数有误，终止运行')
    
    def executeQueryTask(self):
        
        #debug hql by debugger
		#print "executeQueryTask ing 
        #print self.hql
        
        #如果需要输出
        if(self.output == 'true'):
            HiveUtils.execute_shell(self.hql, True, self.filepath)
        #如果不需要输出
        else :
            ret = HiveUtils.execute_shell(self.hql)
            #print ret
		#print ret
		#print ("executeQueryTask Done")

    def calculate(self):
        
        try:
            #解析配置文件，获取运行参数
            self.resolveTheConf()
        except Exception:
            print "运行参数有误，中断执行"
            return
        
        #执行查询任务
        self.executeQueryTask();
        
        

if __name__ == '__main__':
    
    aa = CalQuery()  #direct run will trigger error here
    #aa.resolveTheConf()
    aa.calculate()
    
    #done ,配置好了 Query.xml就可以运行，期中输出路径必须设置
    
    pass
