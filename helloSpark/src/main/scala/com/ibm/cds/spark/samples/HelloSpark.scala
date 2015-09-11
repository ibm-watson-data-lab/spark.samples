/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.cds.spark.samples

import org.apache.spark._

object HelloSpark {
  
  //main method invoked when running as a standalone Spark Application
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Hello Spark")
    val spark = new SparkContext(conf)

    println("Hello Spark Demo. Compute the mean and variance of a collection")
    val stats = computeStatsForCollection(spark);
    println(">>> Results: ")
    println(">>>>>>>Mean: " + stats._1 );
    println(">>>>>>>Variance: " + stats._2);
    spark.stop()
  }
  
  //Library method that can be invoked from Jupyter Notebook
  def computeStatsForCollection( spark: SparkContext, countPerPartitions: Int = 100000, partitions: Int=5): (Double, Double) = {    
    val totalNumber = math.min( countPerPartitions * partitions, Long.MaxValue).toInt;
    val rdd = spark.parallelize( 1 until totalNumber,partitions);
    (rdd.mean(), rdd.variance())
  }
}
