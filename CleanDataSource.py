 #! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。


#Purpose:使用pandas 清洗数据
#
#
#Author：bob jie
#2017.10.30

import pandas
import os,sys
import time
import pandas as pd
import math
from SaveMySql import SaveMySql as Mysql
from TushareStock import TushareStock as StockInfo
from concurrent.futures import ThreadPoolExecutor,wait 
from SaveMyXlsx import SaveMyXlsx as MyXlsx
from WindStock import WindStock as WindInfo


#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class CleanDataSource(object):

	"""读取文档"""
 	def ReadCSV(self,file_name):
 		#首先获取当前路径 2017-11-28处理文件屏蔽掉代码
 		# dic = sys.path[0]+'/'+time.strftime('%Y-%m-%d',
 		# 		time.localtime(time.time()))+'/'
 		# dic = sys.path[0]+'/'+'2017-09-25'+'/'
 		# data = pandas.read_csv(dic+file_name)
 		data = pandas.read_csv(file_name)
 		return data

 	"""机构买入次数>零 机构卖出次数=0 (1.说明机构在吸取筹码 2.机构还没有放盘)"""
 	def CleanData_One(self,file_name):
 		datasOne = CleanDataSource().ReadCSV(file_name)
 		new_datesOne = datasOne[(datasOne.bamount>0)
 							&(datasOne.scount==0)]
 		# print new_datesOne
 		return new_datesOne

 	"""机构成交明细"""
 	def CleanData_Two(self,file_name):
 		datesTwo =  CleanDataSource().ReadCSV(file_name)
 		# print datesTwo()
 		new_datesTwo = datesTwo
 		return new_datesTwo

 	"""每日龙虎榜 买入成交额>%0.2"""
 	def CleanData_Three(self,file_name):
 		datesThree = CleanDataSource().ReadCSV(file_name)
 		# print datesThree
 		new_datesThree = datesThree[datesThree.bratio>=0.2]
 		print new_datesThree
 		return new_datesThree

	"""涨跌幅超过%1 并且 换手率到%20"""
 	def CleanDate_Throue(self,file_name):
 		datesThroue = CleanDataSource().ReadCSV(file_name)
 		new_datesThroue = datesThroue[(datesThroue.turnoverratio<=20)
 				&(datesThroue.changepercent>=-2)
 				&(datesThroue.changepercent<=0)]
 		# print new_datesThroue
 		return new_datesThroue

 	
 	"""集中处理数据仓"""
 	def CleanAllData(self):
 		"""涨跌幅超过%1 并且 换手率到%20"""
 		datas = CleanDataSource().CleanDate_Throue('todayall.csv')
 		"""机构买入次数>零 机构卖出次数=0 (1.说明机构在吸取筹码 2.机构还没有放盘)"""
 		datas = CleanDataSource().CleanData_One('insttops.csv')
 		"""机构成交明细"""
 		datas = CleanDataSource().CleanData_Two('instdetail.csv')

 	"""循环读取code 拉取股票信息（基础信息）"""
	def CleanDataChenjie(self,data,indexs):
		"""处理没有上市股票的异常操作"""
		if data.loc[indexs].values[16] == 0:print "this stock don't timeToMarket"
		#else:StockInfo().Get_k_data(data.loc[indexs].values[1],"2015-09-01","2017-10-31") 修改 2017-11-24 bob—jie 修改数据源的长度
		else:StockInfo().Get_k_data(data.loc[indexs].values[1],"1984-10-01","2017-11-24")
		print "===============%d data finish=================="%(indexs)


	"""集中洗数据数据仓 (基础数据)"""
	def CleanData(self):
		count = 1
		#如果，确认这个表存在 2017-10-30
		# StockInfo().Getstock_basics()
		data = Mysql().ReadMySql('Stock_Dasic_Data','stock_basics')
		pool = ThreadPoolExecutor(max_workers=40)  # 创建一个最大可容纳2个task的线程池(开启线程池)
		futures = []
		for indexs in data.index:
			futures.append(pool.submit(
				CleanDataSource().CleanDataChenjie,data,indexs))
		print wait(futures)

	"""循环读取code 拉取股票信息（全部信息）2017.10.30因为需要5日 10日 20日数据增加接口"""
	def CleanDataChenjieAll(self,data,indexs):
		"""处理没有上市股票的异常操作"""
		time.sleep(1)
		print data.loc[indexs].values[1]
		if data.loc[indexs].values[16] == 0:print "this stock don't timeToMarket"
		else:StockInfo().Get_5_10_20_data(data.loc[indexs].values[1],"2016-11-02","2017-11-24")
		print "===============%d data finish=================="%(indexs)

	"""通过get获取历史的市盈率2017-11-21 bob_jie"""
	def CleanDataPeAll(self,data,indexs):
		#测试代码
		# if data.loc[indexs].values[1][0]=='3' or data.loc[indexs].values[1][0]=='0':
		# 	print "===============%d data finish=================="%(indexs)
		# else:
		if data.loc[indexs].values[16] == 0:print "this stock don't timeToMarket"
		else:StockInfo().Get_Pe_data(data.loc[indexs].values[1],'20110101','20171124') 
		print "===============%d data finish=================="%(indexs)
			



	"""集中洗数据数据仓 （全部信息）2017.10.30因为需要5日 10日 20日数据增加接口"""
	def CleanDataAll(self):
		count = 1
		#如果，确认这个表存在 2017-10-30
		# StockInfo().Getstock_basics()
		data = Mysql().ReadMySql('Stock_Dasic_Data','stock_basics')
		pool = ThreadPoolExecutor(max_workers=40)  # 创建一个最大可容纳2个task的线程池(开启线程池)
		futures = []
		for indexs in data.index:
			futures.append(pool.submit(
				CleanDataSource().CleanDataChenjieAll,data,indexs))
		print wait(futures)


	"""写去股票板块信息"""
	def IntoData(self):
		#获取行业信息
		c_name,c_code,c_code_cname = MyXlsx().ReadXlsx('c_name.xlsx')
		#获取股票列表信息
		data = Mysql().ReadMySql('Stock_Dasic_Data','stock_basics')
		#合并行业信息和股票列表信息
		results = data.merge(c_name,left_on='code',right_on='code',how='outer')
		#20177-11-23修复数据没有的情况下默认值的问题
		result = results.fillna('0')
		# result.to_csv('chenjietest.csv')
		Mysql().SaveAppendMySql(result,'Stock_Dasic_Data','stock_basics')
		Mysql().SaveAppendMySql(c_code,'Stock_Dasic_Data','stock_plet_basics')
		Mysql().SaveAppendMySql(c_code_cname,'Stock_Dasic_Data','stock_plate')
		print "=============== data finish=================="

	"""拉去5日 10日 20日数据"""

	"""获取股票市盈率"""
	def PeData(self):
		#获取股票列表信息
		data = Mysql().ReadMySql('Stock_Dasic_Data','stock_basics')
		#开启多线程
		pool = ThreadPoolExecutor(max_workers=40)  # 创建一个最大可容纳2个task的线程池(开启线程池)
		futures = []
		for indexs in data.index:
			futures.append(pool.submit(
				CleanDataSource().CleanDataPeAll,data,indexs))
		print wait(futures)
		print "=============== data finish=================="


	# """统计板块中股票的个数"""
	# def BanKuaiGeShuData(self):
	# 	#调取统计代码
	# 	StockInfo().Get_BanKui_data('Stock_Dasic_Data','stock_basics')

	"""获取股票全市场基金数据"""
	def StockBasicsData(self):
		StockInfo().Getstock_basics()
		print "=============== data finish=================="

	"""处理买入量 买入额的数据从csv文件中读取写入数据库 2017-11-28 bob_jie"""
	def ReadBasicsData(self):
		datastwo = pd.DataFrame()
		#从csv读取数据
		dataonE = CleanDataSource().ReadCSV('1.csv')
		datatwO = CleanDataSource().ReadCSV('2.csv')
		datathreE = CleanDataSource().ReadCSV('3.csv')

		DATA = dataonE.merge(datatwO, left_on='DateTime', right_on='DateTime', how='outer')
		DATAtwo = DATA.merge(datathreE, left_on='DateTime', right_on='DateTime', how='outer')


		datafouR = CleanDataSource().ReadCSV('4.csv')
		datafivE = CleanDataSource().ReadCSV('5.csv')
		datasiX = CleanDataSource().ReadCSV('6.csv')

		DATAthree = datafouR.merge(datafivE, left_on='DateTime', right_on='DateTime', how='outer')
		DATAfour = DATAthree.merge(datasiX, left_on='DateTime', right_on='DateTime', how='outer')

		dataSeveN = CleanDataSource().ReadCSV('7.csv')
		dataeighT = CleanDataSource().ReadCSV('8.csv')
		dataninE = CleanDataSource().ReadCSV('9.csv')

		DATAfive = dataSeveN.merge(dataeighT, left_on='DateTime', right_on='DateTime', how='outer')
		DATAseven = DATAfive.merge(dataninE, left_on='DateTime', right_on='DateTime', how='outer')


		# #测试代码
		# CleanDataSource().CleanDataChenjieOne('000001.SZ',DATAtwo,DATAfour,DATAseven)
		CleanDataSource().CleanDataChenjieOne('000022.SZ',DATAtwo,DATAfour,DATAseven)

		#开启多线程
		# pool = ThreadPoolExecutor(max_workers=40)  # 创建一个最大可容纳2个task的线程池(开启线程池)
		# futures = []
		# for code in DATAtwo.columns[1:]:
		# 	futures.append(pool.submit(
		# 		CleanDataSource().CleanDataChenjieOne,code,DATAtwo,DATAfour,DATAseven))
		# print wait(futures)
		# print "=============== data finish=================="

	"""读取买入量 买入额的数据数据存取数据库 2017-11-28 bob_jie"""
	def CleanDataChenjieOne(self,code,DATAtwo,DATAfour,DATAseven):
		datastwo = pd.DataFrame()
		datasthree = pd.DataFrame()
		#正式代码
		# DATAold = Mysql().ReadMySql('Stock_Basics_Info_All_New',code.split('.')[0]+'stock_basics')
		#调试代码test
		DATAold = Mysql().ReadMySql('test',code.split('.')[0]+'stock_basics')

		DateTimeOne = DATAtwo['DateTime']

		datastwo['date'] =  DATAtwo['DateTime']
		datastwo['BuyingVolume'] = DATAtwo[code]
		datastwo['SellingVolume'] = DATAfour[code]

		datasthree['date'] = DATAseven['DateTime']
		datasthree['amount'] = DATAseven[code]


		resultees = DATAold.merge(datasthree,left_on='date',right_on='date',how='outer')

		results = resultees.merge(datastwo,left_on='date',right_on='date',how='outer')
		results.drop('index',axis=1, inplace=True)

		newresults = results.dropna(axis=0,thresh=3)

		newresults['OrgVolume'] = newresults['BuyingVolume']+newresults['SellingVolume']

		print newresults

		#处理残缺数据 2017-11-29 bob_jie
		#调试代码test
		# Mysql().SaveAppendMySql(results.dropna(axis=0,thresh=3),'Stock_Basics_Info_All_New',code.split('.')[0]+'stock_basics')
		# Mysql().SaveAppendMySql(newresults,'test',code.split('.')[0]+'stock_basics')
		print "===============%s data finish=================="%(code)

		"""拉取交易日信息进入数据库 2017-11-30 bob_jie"""
	def CleanDataTradeCal(self):
		StockInfo().Get_TradeCal_data("19841001","20171124")
		print "=============== data finish=================="


	"""拉取wind拉取机构信息信息进入数据库 2017-12-13 bob_jie """
	def CleanDataOrgInfo(self,data,indexs):
		if data.loc[indexs].values[16] == 0:print "this stock don't timeToMarket"
		else:
			#最小的日期-1
			StartTime = "2014-01-01"
			EndTime = "2015-08-02"
			code = data.loc[indexs].values[1]
			# InfoDate = Mysql().ReadMySql('test3',code+'stock_org_basics')
			InfoDate = Mysql().ReadMySqlTwo('test3', 'stock_org_basics_'+code)
			if  EndTime == str(InfoDate['min(a.index)'][0]):
				WindInfo().Get_pct_chg_Info(code,
						"MFD_BUYAMT_D,MFD_SELLAMT_D,MFD_NETBUYAMT,MFD_BUYVOL_D,MFD_SELLVOL_D,MFD_NETBUYVOL,MFD_BUYAMT_A,MFD_SELLAMT_A,MFD_NETBUYAMT_A,MFD_BUYVOL_A,MFD_SELLVOL_A,MFD_NETBUYVOL_A,CLOSE,PCT_CHG,TOTAL_SHARES,FREE_FLOAT_SHARES,VOLUME, AMT,PE_TTM,VAL_PE_DEDUCTED_TTM",
						StartTime,
						EndTime,
						"unit=1;traderType=1;PriceAdj=F")
			if  EndTime == str(InfoDate['min(a.index)'][0]):
                WindInfo().Get_pct_chg_Info(code,
						"MFD_BUYAMT_D,MFD_SELLAMT_D,MFD_NETBUYAMT,MFD_BUYVOL_D,MFD_SELLVOL_D,MFD_NETBUYVOL,MFD_BUYAMT_A,MFD_SELLAMT_A,MFD_NETBUYAMT_A,MFD_BUYVOL_A,MFD_SELLVOL_A,MFD_NETBUYVOL_A,CLOSE,PCT_CHG,TOTAL_SHARES,FREE_FLOAT_SHARES,VOLUME, AMT,PE_TTM,VAL_PE_DEDUCTED_TTM",
						StartTime,
						EndTime,
						"unit=1;traderType=1;PriceAdj=F")
				print "code"
				print "===============%d data data cunzai finish==================" % (indexs)
		print "code"
		print "===============%d data finish=================="%(indexs)

	"""拉取wind拉取机构信息信息进入数据库 2018-02-28 bob_jie """
	def CleanDataOrgInfoOne(self, data, indexs):
		if data.loc[indexs].values[16] == 0:
			print "this stock don't timeToMarket"
		else:
			# 最小的日期-1
			StartTime = "2017-08-03"
			EndTime = "2017-08-03"
			# code = data.loc[indexs].values[1]
			code = '000001'
			# InfoDate = Mysql().ReadMySql('test3',code+'stock_org_basics')
			InfoDate = Mysql().ReadMySqlTwo('test3', 'stock_org_basics_'+code)
			if '2017-12-19' == str(InfoDate['max(a.index)'][0]):
				WindInfo().Get_pct_chg_Info(code,
						"",
						StartTime,
						EndTime,
						"unit=1;traderType=1;PriceAdj=F")
			else:
					print "code"
					print "===============%d data data cunzai finish==================" % (indexs)
			print "code"
			print "===============%d data finish==================" % (indexs)


	"""拉取wind拉取机构信息信息进入数据库 2018-02-28 bob_jie """
	def CleanDataOrgInfoTwo(self, data, indexs):
		if data.loc[indexs].values[16] == 0:
			print "this stock don't timeToMarket"
		else:
			# 最小的日期-1
			StartTime = "2014-01-01"
			EndTime = "2017-12-27"
			# code = data.loc[indexs].values[1]
			code = '000001'
			# InfoDate = Mysql().ReadMySql('test3',code+'stock_org_basics')
			InfoDate = Mysql().ReadMySqlTwo('test3', 'stock_org_basics_'+code)
			WindInfo().Get_pct_chg_Info(code,
						"high,low",
						StartTime,
						EndTime,
						"unit=1;traderType=1;PriceAdj=F")
			print "code"
			print "===============%d data finish==================" % (indexs)

	"""拉取wind拉取机构信息信息进入数据库 2017-12-13 bob_jie"""
	def OrgData(self):
		#获取股票列表信息
		data = Mysql().ReadMySql('test4','stock_basics')

		for indexs in data.index:
			try:
				CleanDataSource().CleanDataOrgInfoTwo(data, indexs)
			except:
				continue;
		print "=============== data finish=================="

	"""拉取wind拉取机构信息信息进入数据库 2017-12-13 bob_jie """
	def CleanDataOrgInfoTwo(self, data, indexs):
		if data.loc[indexs].values[16] == 0:
			print "this stock don't timeToMarket"
		else:
			# 最小的日期-1
			StartTime = "2017-12-28"
			EndTime = "2018-02-07"
			code = data.loc[indexs].values[1]
			WindInfo().Get_pct_chg_Info(code,
											"MFD_BUYAMT_D,MFD_SELLAMT_D,MFD_NETBUYAMT,MFD_BUYVOL_D,MFD_SELLVOL_D,MFD_NETBUYVOL,MFD_BUYAMT_A,MFD_SELLAMT_A,MFD_NETBUYAMT_A,MFD_BUYVOL_A,MFD_SELLVOL_A,MFD_NETBUYVOL_A,CLOSE,PCT_CHG,TOTAL_SHARES,FREE_FLOAT_SHARES,VOLUME, AMT,PE_TTM,VAL_PE_DEDUCTED_TTM",
											StartTime,
											EndTime,
											"unit=1;traderType=1;PriceAdj=F")
			print "==============================================="
			print "=====================%s========================" % (code)
			print "===============%d data finish==================" % (indexs)

	"""计算倍数50个字段 2018-03-02"""
	def CleanDataOrg(self):
		count = 1
		#如果，确认这个表存在 2017-10-30
		# StockInfo().Getstock_basics()
		# data = Mysql().ReadMySql('test3','stock_basics')
		data = Mysql().ReadMySql('stock_gainian','shaixuan')
		pool = ThreadPoolExecutor(max_workers=2)  # 创建一个最大可容纳2个task的线程池(开启线程池)
		futures = []
		for indexs in data.index:
			futures.append(pool.submit(
				CleanDataSource().CleanDataOrgInfoThree,data,indexs))
		print wait(futures)


	"""计算归一值 2018-02-21"""
	def JshuanGY(self):
		data = Mysql().ReadMySql('test4','stock_basics')
		# data = Mysql().ReadMySql('stock_gainian','shaixuan')
		pool = ThreadPoolExecutor(max_workers=0)# 创建一个最大可容纳2个task的线程池(开启线程池)
		futures = []
		errcode = []
		i=0
		for code in data['code']:
			try:
				StockInfo().Get_guiyi(code)
				print "===============%d data finish=================="%(i)
				i+=1
			except:
				errcode.append(code)
		print wait(futures)
		print "=============== data finish=================="

	"""计算倍率 2018-03-01 bob_jie """
	def CleanDataOrgInfoThree(self, data, indexs):
		try:
			# code = '300026'
			code = data.loc[indexs].values[0]
			print code
			StockInfo().Get_gnbk(code)
			print "==============================================="
			print "=====================%s========================" % (code)
			print "===============%d data finish==================" % (indexs)
		except Exception as e:
			raise e

if __name__ == '__main__':
# 	CleanDataSource().ReadCSV(file_name="insttops.csv")
# 	file_name="todayall.csv"
# 	file_name="toplist.csv"
# 	file_name="toplist.csv"
# 	CleanDataSource().CleanData_One(file_name)
# 	CleanDataSource().CleanData_Two(file_name)
# 	CleanDataSource().CleanData_Three(file_name)
# 	CleanDataSource().CleanAllData()
# 	CleanDataSource().CleanData() 股票基础数据
# 	CleanDataSource().IntoData() 股票板块信息
# 	CleanDataSource().CleanDataAll()
# 	CleanDataSource().PeData() 股票市盈率
# 	CleanDataSource().StockBasicsData() 获取全市场股票信息
# 	CleanDataSource().CleanDataChenjieOne() 获取机构交易额
# 	CleanDataSource().ReadBasicsData() 获取股票机构买入量 买入额的数据 计算机构当日成交量
# 	CleanDataSource().CleanDataTradeCal() 拉取交易日信息
# 	CleanDataSource().OrgData() 拉取机构
# 	CleanDataSource().JshuanGY()#归一值
#	CleanDataSource().CleanDataOrg()#计算倍数

