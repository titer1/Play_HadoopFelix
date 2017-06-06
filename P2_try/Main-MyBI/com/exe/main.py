#-*-coding:utf8-*-
'''
Created on 2014-3-21

@author: liwei
'''

'''
from com.cal.CalTest import CalSalesCount
from com.util.Constant import PROJECT_CONF

'''

from com.util.pro_env import * 
from com.cal.Basic import BasicCalculate

import sys,getopt
import datetime

from xml.etree import ElementTree as ET



if __name__ == '__main__':
    reload(sys)
    #sys.setdefaultencoding('utf-8')
    type = "main"
   
    today = datetime.date.today()
    yestoday = today + datetime.timedelta(-1)
    ds = yestoday.strftime('%Y-%m-%d')
    opts, args = getopt.getopt(sys.argv[1:], "d:t:")
     
    for op, value in opts:
        if op == "-d":
            ds = value
        elif op == "-t":
            type = value
    
    
    '''
           加载主配置文件
    '''       
    xmlTree = ET.parse(PROJECT_CONF_DIR + "Main.xml")
    print("加载" + PROJECT_CONF_DIR + "Main.xml，当前可执行的type为" + type)
    eles = xmlTree.findall('./process')
    
    for e in eles:
        if e.attrib.get('type') == type : 
            '''
                               判断是否执行
            '''
            b = 'true'
            tasks = []
            for task in e.getchildren():
                tasks.append(task.text)
            #print globals()
            
            '''
                              该函数只能在POSIX平台调用
            pid = os.fork()
            if pid == 0:
            '''
            for task in tasks:
                try:
                    cal = globals()[task]
                    cal.calculate()
                except Exception, e:
                    #print task
                    cal = BasicCalculate()
                    cal.calculate()

                
                
                
                
            
            
            
        
    
       
            
    
            
    
    