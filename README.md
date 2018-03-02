功能

根据errocode的返回值设计接口用例集，检查接口的容错性。

依赖

apache-jmeter-xx apache-ant-xx

windows下 直接运行 install.bat 安装依赖

##有问题反馈 在使用中有任何问题，欢迎反馈给我，可以用以下联系方式跟我交流

邮件(chenjiejiji#gmail.com, 把#换成@)
QQ: 539901741
作者:bob_jie
##结构

	-CleanDataSource 使用pandas 清洗数据
	-Formula 计算公式
	-LibRequests 接口获取的数据
	-ProcessingData 使用tushare
	-SaveMySql 读取数据库配置文件 读写数据库
	-SaveMyXlsx 读取xlcs获取板块信息文件 读写数据库
	-TushareStock 使用tushare
	-WindStock 使用wind
	
##启动 移动到当前文件夹下执行ant


##更新

	-增加归一值计算
	-增加大单净买入量与昨日的倍数
	-大单买入量与昨日的倍数
	-大单主动买入量与昨日的倍数
	-大单净主动买入量与昨日的倍数
	-全量与昨日的倍数
	-10天内最大涨幅
	-10天内最大跌幅
	-10天中到大最大涨幅所用天数
	-截止到第10天的涨跌幅