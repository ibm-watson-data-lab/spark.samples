package com.ibm.cds.spark.samples

import org.apache.spark._
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions

object HelloSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hello Spark")
    val spark = new SparkContext(conf)

    println("Hello Spark Demo. Compute the mean and variance of a randomly generated collection")
    println("Usage: computeStatsForRandomCollection( sparkContext, countPerPartitions, partitions")
    val stats = computeStatsForRandomCollection(spark);
    println(">>> Results: ")
    println(">>>>>>>Mean: " + stats._1 );
    println(">>>>>>>Variance: " + stats._2);
    spark.stop()
  }
  
  def computeStatsForRandomCollection( spark: SparkContext, countPerPartitions: Int = 100000, partitions: Int=5): (Double, Double) = {    
    val totalNumber = math.min( countPerPartitions * partitions, Long.MaxValue).toInt;
    val rdd = spark.parallelize( 1 until totalNumber,partitions);
    (rdd.mean(), rdd.variance())
  }
}
