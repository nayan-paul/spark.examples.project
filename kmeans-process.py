import traceback
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans

#spark-submit --master local kmeans-process.py
#spark-submit --master yarn-cluster kmeans-process.py 

##############################################################################################################
#M2
def mapData(line):
	try:
		return np.array([float(x) for x in line.split(',')])
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'End of process...'
	#finally
#M2
###############################################################################################################

if __name__=="__main__":
	context= SparkContext(appName='KMeans example')
	try:
		data = context.textFile('hdfs://10.0.215.232:8020/data/kmeans/kmeans-data.txt')
		inputRDD = data.map(mapData)
		
		model = KMeans.train(inputRDD,2)
		print "................................center " + str(model.clusterCenters)
		print "................................cost "+str(model.computeCost(inputRDD))
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of Process...'
	#finally
#if