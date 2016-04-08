import sys
from pyspark import SparkContext

def computeStatsForCollection(sc,countPerPartitions=100000,partitions=5):
	totalNumber = min( countPerPartitions * partitions, sys.maxsize)
	rdd = sc.parallelize( range(totalNumber),partitions)
	return (rdd.mean(), rdd.variance())
	
if __name__ == "__main__":
	sc = SparkContext(appName="Hello Spark")
	print("Hello Spark Demo. Compute the mean and variance of a collection")
	stats = computeStatsForCollection(sc);
	print(">>> Results: ")
	print(">>>>>>>Mean: " + str(stats[0]));
	print(">>>>>>>Variance: " + str(stats[1]));
	sc.stop()