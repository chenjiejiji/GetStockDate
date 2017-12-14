#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。
# -*- coding:cp936 -*-


#Purpose:读取xlcs获取板块信息文件 读写数据库
#
#
#Author：bob jie
#2017.10.17

import pandas as pd
import os,sys
import time
import xlrd


#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class SaveMyXlsx(object):
	"""读取数据库配置文件"""	
	def ReadXlsx(self,file_name):
		book = xlrd.open_workbook(file_name)
		datas = pd.DataFrame()
		new_datas = pd.DataFrame()
		new_datastwo = pd.DataFrame()
		datastwo = pd.DataFrame()
		datasthree = pd.DataFrame()		
		

		c_name_a = []
		code_a = []

		code_s = []
		c_name_s = []
		
		for count in range(0,book.nsheets):
			sh = book.sheet_by_index(count)
			c_name = sh.name
			#板块的名称
			for counts in range(1,sh.nrows):
				#这是股票代码
				code = sh.cell_value(rowx=counts, colx=0)
				#写入数据库
				c_name_a.append(sh.name+',')
				c_name_s.append(c_name)
				code_a.append(code)
				code_s.append(code+',')

		datas['code'] = code_a
		datas['c_name'] = c_name_a

		#对数据做筛选统计
		newdate = datas.groupby('code').sum()

		#取索引的值和具体的数据值生成新的列表
		new_datas['code'] = newdate.index
		new_datas['c_name'] = newdate['c_name'].values

		##################################################
		###2017-11-24 增加板块个数的对应表
		datastwo['c_name'] = c_name_s
		datastwo['code'] = code_s

		#对数据做筛选统计
		new_data_two = datastwo.groupby('c_name').sum()
		geshu = datastwo.groupby('c_name').size()

		#取索引的值和具体的数据值生成新的列表
		new_datastwo['c_name'] = new_data_two.index
		new_datastwo['code'] = new_data_two['code'].values
		new_datastwo['count'] = geshu.values

		# new_datastwo.sort_index(axis=1,ascending=False)
		
		##################################################
		#开始打印输出
		# c_name_s
		# code_a

		datasthree['code'] = code_a
		datasthree['plate_name'] = c_name_s


		return (new_datas,new_datastwo,datasthree)