#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。
# -*- coding:cp936 -*-


#Purpose:计算公式
#
#
#Author：bob jie
#2017.10.31

import pandas as pd
import os,sys
import time



#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class Formula(object):
	###第一个计算公式：当日涨跌幅 2017-11-01 增加了自己计算5日 10日的数据
	def FormulaOne(self,data):
		new_data = data
		DailyFluctuation = []
		
		Ma5 = []
		Ma10 = []
		Ma20 = []
		v_ma2 = []
		v_ma5 = []
		v_ma10 = []
		v_ma20 = []

		d_fa2 = []
		d_fa5 = []
		d_fa10 = []
		d_fa20 = []
		d_fa30 = []


		#重新生成index
		new_data = new_data.reset_index(drop=True)

		close = new_data.xs('close',axis=1)
		volume = new_data.xs('volume',axis=1)

		#增加平均2 平均5 批平均10 平均20的均值 2017-11-07
		#增加d_fa2,d_fa5,d_fa10,d_fa20,d_fa30等数据源的汇总 2017-11-07
		for index in range(0,len(close)):
			if index == 0:
				DailyFluctuation.append(0)
				v_ma2.append(0)
			else:
				# bantA = sum(volume[index-1:index+1])/2
				#修改 2017-11-22
				bantA = sum(volume[index-1:index+1])/2
				v_ma2.append(bantA)
				ant = (float(close[index])-float(close[index-1]))/float(close[index-1])
				DailyFluctuation.append(ant)
			if index < 4:
				Ma5.append(0)
				v_ma5.append(0)
			else:
				bantB = sum(volume[index-4:index+1])/5
				v_ma5.append(bantB)
				antA = sum(close[index-4:index+1])/5
				Ma5.append(antA)
			if index < 9:
				Ma10.append(0)
				v_ma10.append(0)
			else:
				antB = sum(close[index-9:index+1])/10
				Ma10.append(antB)
				bantC = sum(volume[index-9:index+1])/10
				v_ma10.append(bantC)
			if index < 19:
				Ma20.append(0)
				v_ma20.append(0)
			else:
				antC = sum(close[index-19:index+1])/20
				Ma20.append(antC)
				bantD = sum(volume[index-19:index+1])/20
				v_ma20.append(bantD)

			if index>(len(close)-3):d_fa2.append(0)
			else:
				CntA = (float(close[index+2])-float(close[index]))/float(close[index])
				d_fa2.append(CntA)
			if index>(len(close)-6):d_fa5.append(0)
			else:
				CntB = (float(close[index+5])-float(close[index]))/float(close[index])
				d_fa5.append(CntB)
			if index>(len(close)-11):d_fa10.append(0)
			else:
				CntC = (float(close[index+10])-float(close[index]))/float(close[index])
				d_fa10.append(CntC)
			if index>(len(close)-21):d_fa20.append(0)
			else:
				CntD = (float(close[index+20])-float(close[index]))/float(close[index])
				d_fa20.append(CntD)
			if index>(len(close)-31):d_fa30.append(0)
			else:
				CntE = (float(close[index+30])-float(close[index]))/float(close[index])
				d_fa30.append(CntE)

		"""生成新的data数据"""
		new_data['ma5'] = Ma5
		new_data['ma10'] = Ma10
		new_data['ma20'] = Ma20
		new_data['DailyFluctuation'] = DailyFluctuation
		new_data['v_ma2'] = v_ma2
		new_data['v_ma5'] = v_ma5
		new_data['v_ma10'] = v_ma10
		new_data['v_ma20'] = v_ma20

		new_data['d_fa2'] = d_fa2
		new_data['d_fa5'] = d_fa5
		new_data['d_fa10'] = d_fa10
		new_data['d_fa20'] = d_fa20
		new_data['d_fa30'] = d_fa30

		return new_data

	"""第二个计算公式：当日涨跌幅 增加code字段 （2017-10-31 修改计算公式）"""
	def FormulaOneAll(self,data,code):
		new_datas = pd.DataFrame()
		new_datas['date'] = data.index

		#合并基本信息
		for colimns_name in data.columns:
			new_datas[colimns_name] = data[colimns_name].values

		#升序排序时间
		new_datas.sort_values(by='date',ascending=True,inplace=True)
		
		#重新生成index
		new_datas = new_datas.reset_index(drop=True)

		DailyFluctuation = []
		Code = []
		close = new_datas.xs('close',axis=1)
		for index in range(0,len(close)):
			if index==0:DailyFluctuation.append(0)
			else:
				ant = (float(close[index])-float(close[index-1]))/float(close[index-1])
				DailyFluctuation.append(ant)

		"""生成新的data数据"""
		new_datas['code'] = code
		new_datas['DailyFluctuation'] = DailyFluctuation

		return new_datas


			