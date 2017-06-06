# -*- coding:UTF-8 -*-
import commands
from com.util.pro_env import HIVE_PATH


class HiveUtils(object):

    def __init__(self):
        pass
   
    @staticmethod
    def execute_shell(hql) :
        
        #将hql语句进行字符串转义
        hql = hql.replace("\"", "'")
        
        #执行查询，并取得执行的状态和输出
        status, output = commands.getstatusoutput(HIVE_PATH + "hive -S -e \"" + hql + "\"")
        
        if status != 0:
            return None
        else:
            print "success"
       
        output = str(output).split("\n")
        
        return output
    
    
    
    
    