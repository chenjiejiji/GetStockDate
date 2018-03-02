#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。
# -*- coding:cp936 -*-


#Purpose:读取数据库配置文件 读写数据库
#
#
#Author：bob jie
#2017.09.26

from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from Formula import Formula
import pandas as pd
import os,sys
import time
import json
from pandas.io.json import json_normalize
from pandas.io import sql

#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class SaveMySql(object):
	"""读取数据库配置文件"""	
	def ReadConfig(self):
		CentOSinfo = []
		df = open('./config.txt','r')
		datas = df.readlines()
		for data in datas:
			CentOSinfo.append(data.replace('\n','').split('=')[1])
		"""服务器信息"""
		return CentOSinfo


	"""储存股票数据存入数据库建表"""
	def SaveMySql(self,data,dbname,tablename):
		Info = SaveMySql().ReadConfig()
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		data.to_sql(tablename,Engine,if_exists='append',dtype={'code':VARCHAR(data.index.get_level_values('code').str.len().max())})

	"""追加股票数据存入数据库建表"""
	def SaveAppendMySql(self,data,dbname,tablename):
		Info = SaveMySql().ReadConfig()
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		data.to_sql(tablename,Engine,if_exists='replace')


	"""储存基本数据数据存入数据库建表(单独新数据)"""
	def SaveMySqlTWO(self,data,dbname,tablename,code=None):
		Info = SaveMySql().ReadConfig()
		"""处理数据的计算公式"""
		if code:new_data = Formula().FormulaOneAll(data,code) 
		else:new_data = Formula().FormulaOne(data)
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		# new_data.to_sql(tablename,Engine,if_exists='replace')
		# new_data.to_sql(tablename,Engine,if_exists='replace',dtype={'date':VARCHAR(data.index.get_level_values('date').str.len().max())})
		new_data.to_sql(tablename,Engine,if_exists='replace')

	"""储存合并市盈率的数据"""
	def SaveMySqlThree(self,data,dbname,tablename,code):
		Info = SaveMySql().ReadConfig()

		#json转dataframe
		for count in range(0,len(data['data'])):
			df = json_normalize(data['data'])
		date=[]
		data = df.xs('tradeDate',axis=1)
		for i in range(0,len(data)):
			date.append(data[i][0:10])

		df['date'] = date
		df.drop('tradeDate',axis=1, inplace=True)
		df.drop('exchangeCD',axis=1, inplace=True)
		df.drop('secID',axis=1, inplace=True)
		df.drop('ticker',axis=1, inplace=True)


		#读取数据库中表的字段
		datatwo = SaveMySql().ReadMySql(dbname,tablename)

		result = pd.merge(datatwo,df,how='left',on=['date','date'])
		result.drop('index',axis=1, inplace=True)
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')

		#存入数据库
		result.to_sql(tablename,Engine,if_exists='replace')

	# """统计每个版块中个股的个数(2017-11-23 bob_jie)"""
	# def SaveMySqlFour(self,data,dbname,tablename):
	# 	Info = SaveMySql().ReadConfig()
	# 	#处理统计个股的支数
	# 	result = Formula().FormulaThree(data)

	# 	# Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
	# 	# #存入数据库
	# 	# result.to_sql(tablename,Engine,if_exists='replace')

	"""增加交易日的数据（2017-11-30 bob_jie)"""
	def SaveMySqlThroue(self,data,dbname,tablename):
		print dbname
		print tablename
		Info = SaveMySql().ReadConfig()

		#json转dataframe
		df = json_normalize(data['data'])

		dataone = df[(df.isOpen==1) & (df.exchangeCD=='XSHE')]

		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		df.to_sql(tablename+'ALL',Engine,if_exists='replace')
		dataone.to_sql(tablename,Engine,if_exists='replace')

	"""增加交易日的数据（2017-12-30 bob_jie)"""
	def SaveMySqlFive(self,code,wsd_data,dbname,tablename):
		Info = SaveMySql().ReadConfig()

		#处理wind数据 生成dataframe
		Requtes=pd.DataFrame(wsd_data.Data,index=wsd_data.Fields,columns=wsd_data.Times)
		Requtes=Requtes.T #将矩阵转置
		Requtes['code'] = code
		
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		Requtes.to_sql(tablename,Engine,if_exists='append')

	"""增加上证指数（2017-12-14 bob_jie)"""
	def SaveMySqlSix(self,code,wsd_data,dbname,tablename):
		Info = SaveMySql().ReadConfig()

		#处理wind数据 生成dataframe
		Requtes=pd.DataFrame(wsd_data.Data,index=wsd_data.Fields,columns=wsd_data.Times)
		Requtes=Requtes.T #将矩阵转置
		Requtes['code'] = code
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		Requtes.to_sql(tablename, Engine, if_exists='append')

	def SaveMySqlSeven(self,dbname,tablename,data):
		Info = SaveMySql().ReadConfig()
		info = Formula().FormulaThrue(data)
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		info.to_sql(tablename, Engine, if_exists='replace')

	def SaveMySqlEight(self,dbname,tablename,data):
		Info = SaveMySql().ReadConfig()
		info = Formula().FormulaFire(data)
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		info.to_sql(tablename, Engine, if_exists='append')


	"""读取数据库获取字段code(正块取出)"""
	def ReadMySql(self,dbname,tablename):
		Info = SaveMySql().ReadConfig()
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		df1 = pd.read_sql(tablename,Engine)
		return df1

	"""sql读取数据库获取字段(正块取出)"""
	def ReadsqlMysql(self,dbname,code):
		Info = SaveMySql().ReadConfig()
		Engine = create_engine('mysql://'+Info[1]+':'+Info[2]+'@'+Info[0]+'/'+dbname+'?charset=utf8')
		sql= "SELECT * FROM test3.stock_org_basics_copy WHERE code="+code+" order by date_time"
		df1 = pd.read_sql(sql,Engine)
		return df1

	"""读取数据库获取字段code（执行单一的sql语句）"""
	def ReadMySqlTwo(self, dbname, tablename):
		Info = SaveMySql().ReadConfig()
		Engine = create_engine('mysql://' + Info[1] + ':' + Info[2] + '@' + Info[0] + '/' + dbname + '?charset=utf8')
		sql = "select max(a.index),min(a.index) from %s a" % (tablename)
		print sql
		# df1 = pd.read_sql(tablename, Engine)
		df1 = pd.read_sql(sql, Engine)
		return df1

	"""读取数据库获取字段code（执行sql语句）"""
	def ReadMySqlThree(self, dbname, tablename):
		Info = SaveMySql().ReadConfig()
		Engine = create_engine('mysql://' + Info[1] + ':' + Info[2] + '@' + Info[0] + '/' + dbname + '?charset=utf8')
		sql = "select * from %s where " % (tablename)
		# df1 = pd.read_sql(tablename, Engine)
		df1 = pd.read_sql('', Engine)
		return df1

