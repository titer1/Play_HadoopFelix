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
		
		#hive -S -e
        
        if status != 0:
			print ("Fail execute_shell")
			return None
        else:
            print "success"
		
		#print str(output)
        output = str(output).split("\n")
	print output
		#print output[0]
		#print "%s" % output
        return output
    
    
    
    
    
