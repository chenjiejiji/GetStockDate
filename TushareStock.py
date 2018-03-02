#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。


#Purpose:使用tushare
#
#
#Author：bob jie
#2017.10.30

import tushare as ts
import time
import os,sys
import time
import shutil
import datetime
from SaveMySql import SaveMySql as Mysql
from LibRequests import LibRequests as Requests
import pandas as pd


#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class TushareStock(object):
	"""写入csv文件"""
 	def SaveCSV(self,bigdata,file_name):
 		#首先获取当前路径
 		dic = sys.path[0]+'/'+time.strftime('%Y-%m-%d',time.localtime(time.time()))+'/'
 		#如果有当前路径有这个文件夹就不创建这个文件夹 没有就创建这个文件,有这个文件夹就直接清空文件夹中的内容
 		if os.path.exists(dic) == False:
 			os.makedirs(dic)
 		else:
 			try:
 				#如果 文件存在 就删除文件，不存在就保存文件到当前文件夹中
 				if os.path.exists(dic+file_name) == True: 
 					os.remove(dic+file_name) 
 					bigdata.to_csv(dic+file_name)
				else:bigdata.to_csv(dic+file_name)
 			except Exception as e:
 				print e.args

	"""获取基本面数据"""
	def Get50sz(self):
		date = ts.get_stock_basics()
		self.SaveCSV(date,'stock_basics.csv')

	"""每日龙虎榜"""	
	def Gettop_list(self):		
		"""今天是星期六或者星期天 不是交易日 周六做异常判断"""
		todays = datetime.date.today()
		dayOfWeek = datetime.date.today().weekday()
		if dayOfWeek==5: timess=todays-datetime.timedelta(days=1)
		elif dayOfWeek==6:timess=todays-datetime.timedelta(days=2)
		else:timess=todays
		dates = ts.top_list('%s'%(timess))
		self.SaveCSV(dates,'toplist.csv')

	"""机构成交交易明细"""
	def GetDetail(self):
		datatts = ts.inst_detail()
		self.SaveCSV(datatts,'instdetail.csv')

 	"""买入席位 默认拉去5日的买入 卖出席位数"""
 	def GetCaptops(self):
 		datateee = ts.cap_tops()
 		self.SaveCSV(datateee,'captops.csv')

 	"""机构席位跟踪"""
	def GetInsttops(self):
 		datateees = ts.inst_tops()
 		self.SaveCSV(datateees,'insttops.csv')

 	"""获取创业板的数据"""
 	def Getgemclassified(self):
 		datateeee = ts.get_gem_classified()
 		self.SaveCSV(datateeee, 'gemclassified.csv')

 	"""拉取交易所有股票的行情数据"""
 	def Gettoday_all(self):
 		datateeeee = ts.get_today_all()
 		self.SaveCSV(datateeeee, 'todayall.csv')

 	"""沪市融资融券明细数据"""
 	def Getshmargins(self):
 		datateeeeee = ts.sh_margin_details()
 		self.SaveCSV(datateeeeee, 'shmargindetails.csv')

 	"""深市融资融券明细数据"""
 	def Getszmargins(self):
 		datateeeeeee = ts.sz_margin_details()
 		self.SaveCSV(datateeeeeee, 'szmargindetails.csv')

 	"""拉取全部的股票信息"""
 	def Getstock_basics(self):
 		datateeeeeeee = ts.get_stock_basics()
 		# self.SaveCSV(datateeeeeeee, 'getstockbasics.csv')
 		#2017-11-27更新了这块代码
 		Mysql().SaveMySql(datateeeeeeee,
 			'Stock_Dasic_Data','stock_basics')

 	"""获取所有的历史数据"""
 	def Get_hist_data(self,code):
 		data_h_data_all = ts.get_h_data(code)
 		Mysql().SaveMySqlTWO(data_h_data_all,
 			'Stock_Basics_Info',code+'stock_basics')


 	"""获取3年历史数据 （2017-11-01 修改了适配代码）"""
 	def Get_k_data(self,code,start,end):
 		data_h_data = ts.get_k_data(code,autype='qfq',start=start,end=end)
 		# #正式代码了这里
 		Mysql().SaveMySqlTWO(data_h_data,
 			'Stock_Basics_Info_All_New',code+'stock_basics')
 		# #调试代码
 		# Mysql().SaveMySqlTWO(data_h_data,
 		# 	'test',code+'stock_basics')


 	"""拉去5日 10日 20日数据(2017-10-30增加全部数据接口)"""
 	def Get_5_10_20_data(self,code,start,end):
 		data_5_data = ts.get_hist_data(code,start=start,end=end)		
 		#防止过多拉取数据源 网站屏蔽IP	
 		cons = ts.get_apis()
 		# 增加前复权 adj='qfq' 后复权 adj='hfq'（bob_jie ： 2017-11-01) 
 		data_5_data = ts.bar(code,conn=cons,start_date=start,adj='qfq',end_date=end,ma=[5, 10, 20],factors=['vr', 'tor'])
 		try:
 			data_5_data.values[0][0]
 		except Exception as e:
 			time.sleep(2)
 			#防止过多拉取数据源 网站屏蔽IP	
 			cons = ts.get_apis()
 			# 增加前复权 adj='qfq' 后复权 adj='hfq'（bob_jie ： 2017-11-01) 
 			data_5_data = ts.bar(code,conn=cons,start_date=start,adj='qfq',end_date=end,ma=[5, 10, 20],factors=['vr', 'tor'])
 		ts.get_h_data('002337',autype='qfq') 
 		Mysql().SaveMySqlTWO(data_5_data,'Stock_Basics_Info_All',code+'stock_basics',code)

 	"""拉去pe的数据(2017-11-21增加pe接口的数据源)"""
 	def Get_Pe_data(self,code,start,end):
 		#获取数据源 bob_jie 2017-11-21
 		#深圳的（有了）.XSHE
		#上海的.XSHG
 		if code[0]=='0' or code[0]=='3':codenew = code+'.XSHE'
 		else:codenew = code+'.XSHG'
 		token = "7f47d59fa3d950a1de2026fbbb72220681968623a43123bc4e2d732c7edeec75"
 		URL = 'https://api.wmcloud.com/data/v1//api/market/getMktEquPEJL.json?field=&secID='+codenew+'&startDate='+start+'&endDate='+end
 		Requtes = Requests().GetRequests(URL,headers = {"Authorization": "Bearer " + token,"Accept-Encoding": "gzip, deflate"})
		#存取数据库 bob_jie 2017-11-21 正式代码
		# Mysql().SaveMySqlThree(Requtes,'Stock_Basics_Info_All_New',code+'stock_basics',code)
		#2017-11-27调试代码
		Mysql().SaveMySqlThree(Requtes,'test',code+'stock_basics',code)


	"""获取股票交易日期(2017-11-30增加交易日数据接口)"""
	def Get_TradeCal_data(self,start,end):
 		token = "7f47d59fa3d950a1de2026fbbb72220681968623a43123bc4e2d732c7edeec75"
 		URL = 'https://api.wmcloud.com/data/v1//api/master/getTradeCal.json?field=&exchangeCD=XSHG,XSHE&beginDate='+start+'&endDate='+end
 		# /api/master/getTradeCal.json?field=&exchangeCD=XSHG,XSHE&beginDate=&endDate=
 		Requtes = Requests().GetRequests(URL,headers = {"Authorization": "Bearer " + token,"Accept-Encoding": "gzip, deflate"})

		Mysql().SaveMySqlThroue(Requtes,'Stock_Dasic_Data','stock_TradeCal')

	'''获取历史分笔数据（2017-12-07 增加历史分笔接口）'''
	def Get_Tick_data(self):
		df = ts.get_tick_data('600848',date='2014-01-09')
		print type(df)
		df.to_csv('G:/600848.csv')

	"""拉去板块的数据(2017-11-23增加统计数据的个数)"""
 	def Get_BanKui_data(self,dbname,tablename):
 		#读取数据库板块信息
 		data = Mysql().ReadMySql(dbname,tablename)
 		Mysql().SaveMySqlFour(data,dbname,'stock_basics_plate')

	"""十大股东的数据（2017-12-11 振增加）"""
	def Get_Top10_holders(self):
		df = ts.top10_holders('600848')
		# print df
		# print type(df)
		obj_series_tuple=pd.Series(df,index=list('abcdefgsll'))
		print obj_series_tuple
		# df.to_csv('G:/600848_holders.csv')

	"""计算归一值（2018-02-01)"""
	def Get_guiyi(self,code):
		data = Mysql().ReadMySql('test4','stock_org_basics_'+code)
		Mysql().SaveMySqlSeven('test4','stock_org_basics_'+code,data)

	"""计算概念板块(2018-02-27)"""
	def Get_gnbk(self,code):
		#做第一次操作
		data = Mysql().ReadsqlMysql('test3',code)
		#做第二个值的时候应该切换
		# data = Mysql().ReadMySql('stock_gainian','stock_org_basics_'+code)
		#库要做修改
		Mysql().SaveMySqlEight('stock_gainian','stock_org_basics_new',data)