#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。
# -*- coding:cp936 -*-


#Purpose:使用wind
#
#
#Author：bob jie
#2017.12.13

from SaveMySql import SaveMySql as Mysql
from WindPy import w
import pandas as pd
import datetime
import os,sys

#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class WindStock(object):
	"""拉取机构数据"""
	def Get_Organization_Info(self,code,data,start_data,end_data,expend):
		if code[0]=='0' or code[0]=='3':codenew = code+'.SZ'
 		else:codenew = code+'.SH'
		#启动wind客户端
		WindStock().Init_Wind()

		wsd_data = WindStock().Get_Wind_Info(codenew,data,start_data,end_data,expend)
		#存入数据库
		Mysql().SaveMySqlFive(code,wsd_data,'Organization_Basics_Info',code+'stock_org_basics')

	"""拉取深证 上证数据"""
	def Get_pct_chg_Info(self,code,data,start_data,end_data,expend):
		if code[0]=='0' or code[0]=='3':codenew = code+'.SZ'
 		else:codenew = code+'.SH'
		#启动wind客户端
		WindStock().Init_Wind()

		#拉取wind数据
		wsd_data = WindStock().Get_Wind_Info(codenew,data,start_data,end_data,expend)

		#存入数据库
		Mysql().SaveMySqlSix(code,wsd_data,'Stock_Dasic_Data','stock_pct_chg')

	"""调用获取拉取信息端口"""
	def Get_Wind_Info(self,code,data,start_data,end_data,expend):
		Info = w.wsd(code,data,start_data,end_data,expend)

		return Info

	"""启动wind客户端"""
	def Init_Wind(self):
		w.start();