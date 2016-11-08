import traceback
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

#grant all on *.* TO 'root'@'%' IDENTIFIED BY 'Mysql1234*' with grant option;
#flush privileges;
#wget http://www.java2s.com/Code/JarDownload/com.mysql/com.mysql.jdbc_5.1.5.jar.zip
#spark-submit --master local --jars /tmp/com.mysql.jdbc_5.1.5.jar sql-dataaccess-process.py

if __name__=='__main__':
	context = SparkContext(appName='Data Access using mySQL')
	sqlContext = SQLContext(context)
	try:
		dataFrame = sqlContext.read.format('jdbc').option('url','jdbc:mysql://10.0.175.190:3306/classicmodels?user=root&password=Mysql1234*').option("driver", "com.mysql.jdbc.Driver").option('dbtable','(select lastName,firstName,email from employees limit 10) as custom').load()
		
		dataFrame.printSchema()
		
		inputRecords = dataFrame.rdd
		
		for row in inputRecords.take(5):
			print '................'+row.lastName + row.firstName + row.email
		#for
		
		
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of process...'
	#finally
#if
