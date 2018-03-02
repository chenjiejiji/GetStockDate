#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。
# -*- coding:cp936 -*-


#Purpose:计算公式
#
#
#Author：bob jie
#2017.10.31

import pandas as pd
import numpy as np
import math
import os,sys
import time
import re



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

	# """第三个计算公式：统计板块信息（2017-11-23）"""
	# def FormulaThree(self,data):
	# 	code = data.xs('code',axis=1)
	# 	c_name = data.xs('c_name',axis=1)

	# 	codenew = []
	# 	c_namenew = []

	# 	#循环code的值
	# 	for index in range(0,len(code)):
	# 		codeone = code[index]
	# 		if c_name[index] != '0':
	# 			c_nameones = c_name[index][0:-1].split(',')
	# 			[[codenew.append(codeone),c_namenew.append(c_nameone)] for c_nameone in c_nameones]

	# 	df = pd.DataFrame()
	# 	df['code'] = codenew
	# 	df['c_name'] = c_namenew

	# 	print df
	# 	# return df
	# 		# for c_nameone in c_nameones:
	# 		# 	codenew.append(codeone)
	# 		# 	c_namenew.append(c_nameone)

	"""第四个计算公式：计算归一值（2018-02-01）"""
	def FormulaThrue(self,data):
		new_datas = data
		#升序排序时间
		new_datas.sort_values(by='index',ascending=True,inplace=True)

		guiyi = []
		# print new_datas
		for indexs in xrange(0,len(new_datas)):
			if indexs>=29:
				if new_datas['VOLUME'][indexs] == 0:
					print new_datas['VOLUME'][indexs]
					print indexs
					guiyi.append(9)
				else:
					start=0
					if indexs>=360:
						start = indexs-110
					new_data = new_datas[['VOLUME','CLOSE']].iloc[start:indexs+1]
					new_datass = new_data[new_data.VOLUME != 0]
					CLOSE =  list(new_datass['CLOSE'])
					ant = CLOSE[len(CLOSE)-30:len(CLOSE)]
					maxa,mina,dangqinga = max(ant),min(ant),ant[29]
					guiyian = (dangqinga-mina)/(maxa-mina)
					guiyi.append(guiyian)
			else:
				guiyi.append(9)
		new_datas['GUIYIAN'] = guiyi
		return new_datas


		close = new_datas['CLOSE']
		volume = new_datas['VOLUME']
		data = new_datas['index']
		guiyi = []
		for i in xrange(0,len(close)):
			guiyian=0
			if i>=29:
				ant = list(close[i-29:i+1])
				bnt = list(volume[i-29:i+1])
				if bnt[29] == 0:
					guiyi.append(9)
				else:
					if 0 in bnt:
						ant = list(close[i-29-bnt.count(0):i+1])
						bnt = list(volume[i-29-bnt.count(0):i+1])
						antt= []
						[antt.append(ant[count]) for count in xrange(0,len(bnt)) if bnt[count]!=0]
						ant =[]
						ant =antt
					print len(ant)
					print ant
					print ant[29]
					print data[i]
					# print ant[29]
					maxa,mina=max(ant),min(ant)
					dangqinga =	ant[29]
					guiyian = (dangqinga-mina)/(maxa-mina)
					guiyi.append(guiyian)
			else:
				print "111111111111"
				guiyi.append(9)
		new_datas['GUIYI']=guiyi


		# return new_datas

	"""第四个计算公式：计算倍数 均值 计算 1 3 10 20 30（2018-03-01）"""
	def FormulaFire(self,new_data):
		#排序下
		new_data.sort_values(by='date_time',ascending=True,inplace=True)
		newinfo = new_data[new_data['VOLUME']>0]

		#填充空值
		newinfo=newinfo.fillna(0)

		count=0
		for ORGName in ['MFD_NETBUYVOL','MFD_BUYVOL_D','MFD_BUYVOL_A','MFD_NETBUYVOL_A','VOLUME','MFD_BUYVOL_D']:
			names=['MFDNetBuyVolTimes','MFDBuyVolDTimes','MFDBuyVolATimes','MFDNetBuyVolATimes','VolTimes','MFDBuyVolDPctTimes']
			for day in [1,3,10,20,30]:
				#重置一下
				newdata = pd.DataFrame()
				#列表操作
				BUY_LARGE_AMOUN_all=[]

				name_all='%s%dCmrsion'%(names[count],day)

				ORGName_two = ORGName
				ORGName_five = 'FREE_FLOAT_SHARES'

				MFD_NETBUYVOL_A = list(newinfo.xs(ORGName_two,axis=1))
				MFD_NETBUYVOL_E = list(newinfo.xs(ORGName_five,axis=1))

				if ORGName=='MFD_BUYVOL_D':
					for indexs in xrange(0,len(newinfo)):
						#公式:
						#(今日's MFD_BULVOL_D/Free_Float_Shares)/(SUM(每天's MFD_BUYVOL_D in 3天)/3)/Free_Float_Shares)
						#
						if indexs<day or MFD_NETBUYVOL_A[indexs]<=0 or MFD_NETBUYVOL_A[indexs-1]<=0:
							BUY_LARGE_AMOUN_all.append(0)
						else:
							ynt=(MFD_NETBUYVOL_A[indexs]/MFD_NETBUYVOL_E[indexs])/((sum(MFD_NETBUYVOL_A[indexs-day:indexs])/day)/MFD_NETBUYVOL_E[indexs-1])
							BUY_LARGE_AMOUN_all.append(ynt)
				else:
					for indexs in xrange(0,len(newinfo)):
						#公式:
						#(今日's 变量)/((SUM(每天's前几日和 不包含当天)/天数))
						#
						if indexs<day or int(MFD_NETBUYVOL_A[indexs])<=0 or int(sum(MFD_NETBUYVOL_A[indexs-day:indexs]))<=0:
							BUY_LARGE_AMOUN_all.append(0)
						else:
							ent=MFD_NETBUYVOL_A[indexs]/(sum(MFD_NETBUYVOL_A[indexs-day:indexs])/day)
							BUY_LARGE_AMOUN_all.append(ent)

				newdata['date_time']=newinfo['date_time']
				newdata[name_all]=BUY_LARGE_AMOUN_all

				#把数据并到老的数据中
				new_data=new_data.merge(newdata, left_on='date_time', right_on='date_time', how='outer')
				#加一
			count+=1

		for day_two in [10,20,30,40,60]:
			#重置一下
			newdata = pd.DataFrame()
			#列表操作
			BUY_LARGE_AMOUN_two_all=[]
			BUY_LARGE_AMOUN_three_all=[]
			BUY_LARGE_AMOUN_throu_all=[]
			BUY_LARGE_AMOUN_five_all=[]

			name_two='maxPctChg%dDaysPeriod'%(day_two)
			name_throu='maxConsumDays%dDaysPeriod'%(day_two)
			name_three='minPctChg%dDaysPeriod'%(day_two)
			name_five='pctChg%dthDayDue'%(day_two)

			ORGName_two = 'CLOSE'
			ORGName_three = 'date_time'

			MFD_NETBUYVOL_B = list(newinfo.xs(ORGName_two,axis=1))
			MFD_NETBUYVOL_C = list(newinfo.xs(ORGName_three,axis=1))

			for indexs in xrange(0,len(newinfo)):
					if indexs>=(len(newinfo)-day_two-1):
						BUY_LARGE_AMOUN_two_all.append(0)
						BUY_LARGE_AMOUN_three_all.append(0)
						BUY_LARGE_AMOUN_throu_all.append(0)
						BUY_LARGE_AMOUN_five_all.append(0)
					else:
						#公式:
						#(Max(从第二天开始推10天每天's close in 10 days period)-今天's close)/今天's close，单位：百分比
						#
						fw=MFD_NETBUYVOL_B[indexs+1:indexs+day_two+1]
						maxnuber=max(fw)
						maxwz=fw.index(maxnuber)
						fnt=(maxnuber/MFD_NETBUYVOL_B[indexs])-1
						#
						BUY_LARGE_AMOUN_two_all.append(fnt)

						#计算相隔天数
						end=indexs+maxwz+1
						start=indexs
						maxjgday=MFD_NETBUYVOL_C[end]-MFD_NETBUYVOL_C[start]
						BUY_LARGE_AMOUN_throu_all.append(int(re.split(' ',str(maxjgday))[0]))

						#公式:
						#(Min(从第二天开始推10天每天's close in 10 days period)-今天's close)/今天's close，单位：百分比
						#
						gw=MFD_NETBUYVOL_B[indexs+1:indexs+day_two+1]
						minnuber=min(gw)
						minwz=gw.index(maxnuber)
						gnt=(minnuber/MFD_NETBUYVOL_B[indexs])-1
						BUY_LARGE_AMOUN_three_all.append(gnt)

						#公式:
						#(从第二天开始推10天的第10Day's close-今天's close)/今天's close，单位：百分比
						#
						tenday=indexs+day_two
						znt=(MFD_NETBUYVOL_B[tenday]/MFD_NETBUYVOL_B[indexs])-1
						BUY_LARGE_AMOUN_five_all.append(znt)

			newdata['date_time']=newinfo['date_time']
			newdata[name_two]=BUY_LARGE_AMOUN_two_all
			newdata[name_throu]=BUY_LARGE_AMOUN_throu_all
			newdata[name_three]=BUY_LARGE_AMOUN_three_all
			newdata[name_five]=BUY_LARGE_AMOUN_five_all

			#把数据并到老的数据中
			new_data=new_data.merge(newdata, left_on='date_time', right_on='date_time', how='outer')

		new_data=new_data.set_index('date_time')
		return new_data