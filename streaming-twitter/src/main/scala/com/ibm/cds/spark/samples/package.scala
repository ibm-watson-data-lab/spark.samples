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

package com.ibm.cds.spark

/**
 * @author dtaieb
 */
import scala.collection.mutable._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

package object samples {
  
  case class EnrichedTweet( author:String, date: String, lang: String, text: String, lat: Double, long: Double, sentimentScores: Map[String, Double]){
    def toRow():Row={
      var colValues = Array[Any](author,date,lang,text,lat,long)
      val scores = for {
        (_,emotion)<-ToneAnalyzer.sentimentFactors
        score=sentimentScores.getOrElse(emotion, 0.0)
      }yield score
      colValues = colValues ++ scores
      Row(colValues.toArray:_*)
    }
  }
	
  val schemaString = "author date lang text lat:double long:double"
	val schemaTweets =
			StructType(
					schemaString.split(" ").map(
							fieldName => {
								val ar = fieldName.split(":");
								StructField(
										ar.lift(0).get, 
										ar.lift(1).getOrElse("string") match{
											case "int" => IntegerType
											case "double" => DoubleType
											case _ => StringType
										},
										true
                )
							}
					).union( 
							ToneAnalyzer.sentimentFactors.map( f => StructField( f._1, DoubleType )).toArray[StructField]
					)
			)
}