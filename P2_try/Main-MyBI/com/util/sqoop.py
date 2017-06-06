# -*- coding:UTF-8 -*-
#! /usr/bin/env python
'''
Created on 2013-10-21

@author: hp
'''
import sys
from xml.etree import ElementTree as ET
from com.util.pro_env import SQOOP_PATH, PROJECT_CONF_DIR
from xml.dom import minidom
import commands
import os
import traceback
from calendar import main



class SqoopUtil(object):
    '''
    hive operation
    '''
    def __init__(self):
        pass
        
    @staticmethod
    def execute_shell(shell, sqoop_path=SQOOP_PATH) :
            
        #将传入的shell命令执行
        status, output = commands.getstatusoutput(SQOOP_PATH + shell)
        if status != 0:
            return None
        else:
            print "success"
           
        output = str(output).split("\n")
            
        return output
    
    
    #获取sqoop命令
    @staticmethod
    def get_xml_for_sqoop(filepath,type,ds):
        xmlTree = ET.parse(filepath)
        #获取task节点
        taskNodes = xmlTree.findall("./task")
        #获取Sqoop节点
        sqoopNodes = taskNodes[0].findall("./sqoop-shell")
        
        #获取Sqoop命令类型
        sqoopAttr = sqoopNodes[0].attrib["type"]
        #获取
        praNodes = sqoopNodes[0].findall("./param")
        
        #将param的信息以键值对的信息保存到字典
        cmap = {}
        for i in range(len(praNodes)):
            #print i
            key = praNodes[i].attrib["key"]
            value = praNodes[i].text
            cmap[key] = value
            
        #迭代字段将param的信息拼装成字符串
        command = "sqoop " + "--" +  sqoopAttr
        
        import_condition = ""
        if (type == "all"):
            import_condition = "ds <= " + ds
        elif (type == "add"):
            import_condition = "ds >= " + ds
        else:
            raise Exception
        
        for key in cmap.keys():
            value = cmap[key]
             
            if(value == None):
                value = ""
            
            if(key == "query"):
                print "@@@" + value + "@@@"
                print import_condition
                value = value.replace("\$CONDITIONS", import_condition)

            command += " --" + key + " " + value + "\n"
     
        print command
        return command
        
    
#     #根据传入的时间和不同的type拼装成不同的命令，以便实现全量或增量导入
#     def get_shell(self,cmap,type,ds):
#         
#         import_condition = ""
#         
#         if (type == "all"):
#             import_condition = "ds <= " + ds
#         elif (type == "add"):
#             import_condition = "ds >= " + ds
#         else:
#             raise Exception
        
        
        
    
        
    def __del__(self):
        pass

if __name__ == '__main__':
    SqoopUtil.get_xml_for_sqoop(PROJECT_CONF_DIR + "table.xml", "add", "2014-11-11");
    
    
    pass

