# -*- coding:UTF-8 -*-
#*********************************************
#项目的路径
#PROJECT_DIR = "E:\\workspace_eclipse\\CalController\\"

import platform

sysstr = platform.system()
if(sysstr =="Windows"):
#print ("Call Windows tasks")
	PROJECT_DIR = "C:\\QQDownload\\Hadoop_Felix_code\\P2_try\\Main-MyBI\\"
	#项目配置文件的路径
	PROJECT_CONF_DIR = PROJECT_DIR + "conf\\"
	#项目第三方库的路径
	PROJECT_LIB_DIR = PROJECT_DIR + "lib\\"
	#项目临时文件的路径
	PROJECT_TMP_DIR = PROJECT_DIR + "tmp\\"
elif(sysstr == "Linux"):
#print ("Call Linux tasks")
	PROJECT_DIR = "/media/sf_Hadoop_felixP2_try/Main-MyBI/"
	#项目配置文件的路径
	PROJECT_CONF_DIR = PROJECT_DIR + "conf/"
	#项目第三方库的路径
	PROJECT_LIB_DIR = PROJECT_DIR + "lib/"
	#项目临时文件的路径
	PROJECT_TMP_DIR = PROJECT_DIR + "tmp/"	
else:
	print ("Exception ,Not supported os")
	

#*********************************************

# todo usr local change it 

#Hadoop的安装路径
HADOOP_HOME = "/usr/local/hadoop/"
#Hadoop命令的路径
HADOOP_PATH = HADOOP_HOME+"bin"
#HIVE的安装路径
HIVE_HOME = "/usr/local/hive/"
#Hive命令路径
HIVE_PATH = HIVE_HOME + "bin/"
#Sqoop的安装路径
SQOOP_HOME = "/opt/sqoop-1.3.0-cdh3u6/"
#Sqoop的命令路径
SQOOP_PATH = SQOOP_HOME + "bin/"
#*********************************************
#Java的安装路径
JAVA_HOME = "/usr/java/latest/"
