'''
Created on 2014-9-21

@author: Administrator
'''

import os
from com.util.pro_env import HADOOP_HOME, PROJECT_LIB_DIR
import sys


if __name__ == '__main__':
    
    inputPath  = "/tmp/apache_log/" + sys.argv[0]
         
    outputPath = "/user/hive/warehouse/clickstream_log/ds=" + sys.argv[0]

    shell = HADOOP_HOME + "hadoop jar " + PROJECT_LIB_DIR + "clickstream_etl.jar com.etl.mapreduce" + inputPath + " " + outputPath
         
    os.system(shell)
