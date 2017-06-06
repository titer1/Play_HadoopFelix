# -*- coding:UTF-8 -*-s
'''
Created on 2014-8-26

@author: Administrator
'''

from com.util.pro_env import *
from xml.etree import ElementTree


from com.util.sqoop import SqoopUtil
import sys
      
#其中dt为昨天的日期，将由调度模块传入
def resolve_conf(dt):
        
    #获得配置文件名
    conf_file = PROJECT_CONF_DIR + "Import.xml"
    #解析配置文件
    xml_tree = ElementTree.parse(conf_file)
    #获得pras元素
    tasks = xml_tree.findall('./task')

    for task in tasks:
        #获得导入类型，增量导入或者全量导入
        import_type = task.attrib["type"]
         
        #获得表名集合
        tables = task.findall('./table')
        
        #用来保存待执行的Sqoop命令的集合
        cmds = []
        
        #迭代表名集合，解析表配置文件
        for i in range(len(tables)):
            #表名
            table_name = tables[i].text
            #表配置文件名
            table_conf_file = PROJECT_CONF_DIR + table_name + ".xml"
            
            #解析表配置文件
            xmlTree = ElementTree.parse(table_conf_file)
            
            #获取sqoop-shell节点
            sqoopNodes = xmlTree.findall("./sqoop-shell")
            
            #获取sqoop命令类型
            sqoop_cmd_type = sqoopNodes[0].attrib["type"]
            #获取
            praNodes = sqoopNodes[0].findall("./param")
            
            #用来保存param的信息的字典
            cmap = {}

            for i in range(len(praNodes)):
                #获得key属性的值
                key = praNodes[i].attrib["key"]
                #获得param标签中间的值
                value = praNodes[i].text
                #保存到字典中
                cmap[key] = value
                 
                #首先组装成sqoop命令头
                command = "sqoop " + "--" +  sqoop_cmd_type
                    
                #如果为全量导入
                if (import_type == "all"):
                    import_condition = "< " + dt
                #如果为增量导入
                elif (import_type == "add"):
                    import_condition = "= " + dt
                else:
                    raise Exception
                   
                ##迭代字典将param的信息拼装成字符串
                for key in cmap.keys():
                    
                    value = cmap[key]
                       
                    #如果不是键值对形式的命令选项
                    if(value == None or value == "" or value == " "):
                        value = ""
                       
                    #将query的CONDITIONS替换为查询条件
                    if(key == "query"):
                        value = value.replace("\$CONDITIONS", import_condition)
                       
                    #将导入分区替换为传入的时间 
                    if(key == "hive-partition-value"):
                        value = value.replace("$dt", dt)
                    
                    #拼装为命令
                    command += " --" + key + " " + value + "\n"
            
            #将命令加入至待执行命令集合            
            cmds.append(command)     
                    
        return cmds
            
if __name__ == '__main__':
    
    #调度模块将昨天的时间传入
    dt = "2014-10-11"
    #解析配置文件，获得sqoop命令集合
    cmds = resolve_conf(dt)
        
    #迭代集合，执行命令
    for i in range(len(cmds)):
        
        cmd = cmds[i]
        print cmd
        #执行导入过程
        #SqoopUtil.execute_shell(cmd)
        
        ##66 在linux 上打开这段进行执行
        