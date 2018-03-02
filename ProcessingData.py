#! usr/bin/python
#coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用。


#Purpose:使用tushare
#
#
#Author：bob jie
#2017.09.21


#处理编码格式并把当前环境加入path
reload(sys)
sys.setdefaultencoding('utf-8')

class TushareStock(object):
	"""读取数据生成HTML文档"""
 	def SaveCSV(self,bigdata,file_name):
 		#首先获取当前路径
 		dic = sys.path[0]+'/'+time.strftime('%Y-%m-%d',time.localtime(time.time()))
 		#
 		if os.path.exists(dic) == False:
 			os.makedirs(dic)
 		bigdata.to_csv(dic+file_name)
