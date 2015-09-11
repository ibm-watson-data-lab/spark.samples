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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.http4s._
import org.http4s.Http4s._
import org.http4s.Status._
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.headers.Authorization
import com.ibm.couchdb._
import scalaz.concurrent.Task
import twitter4j.Status
import upickle.Js
import org.apache.commons.lang3.StringEscapeUtils
import scalaz.{ \/, -\/, \/-}
import org.http4s.client.Client
import org.json4s._
import scala.collection.mutable._



/**
 * @author dtaieb
 */
object StreamingTwitter {
  private var ssc: StreamingContext = null
  
  //main method invoked when running as a standalone Spark Application
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Streaming Twitter Demo")
    val sc = new SparkContext(conf)
    startTwitterStreaming(sc);
  }
  
  //Hold configuration key/value pairs
  val config = Map[String, String](
      ("twitter4j.oauth.consumerKey", Option(System.getProperty("twitter4j.oauth.consumerKey")).orNull ),
      ("twitter4j.oauth.consumerSecret", Option(System.getProperty("twitter4j.oauth.consumerSecret")).orNull ),
      ("twitter4j.oauth.accessToken", Option(System.getProperty("twitter4j.oauth.accessToken")).orNull ),
      ("twitter4j.oauth.accessTokenSecret", Option(System.getProperty("twitter4j.oauth.accessTokenSecret")).orNull ),
      ("tweets.key", Option(System.getProperty("tweets.key")).getOrElse("")),
      ("cloudant.hostName", Option(System.getProperty("cloudant.hostName")).orNull ),
      ("cloudant.https", Option(System.getProperty("cloudant.https")).orNull ),
      ("cloudant.port", Option(System.getProperty("cloudant.port")).orNull ),
      ("cloudant.username", Option(System.getProperty("cloudant.username")).orNull ),
      ("cloudant.password", Option(System.getProperty("cloudant.password")).orNull ),
      ("watson.tone.url", Option(System.getProperty("watson.tone.url")).orNull ),
      ("watson.tone.username", Option(System.getProperty("watson.tone.username")).orNull ),
      ("watson.tone.password", Option(System.getProperty("watson.tone.password")).orNull )
  )
  
  //Validate configuration settings
  def validateConfiguration() : Boolean = {
    var ret: Boolean = true;
    config.foreach( (t:(String, String)) => 
      if ( t._2 == null ){
        println(t._1 + " configuration not set. Use setConfig(\"" + t._1 + "\",<your Value>)"); 
        ret = false;
      }
    )
    
    ret
  }
  
  def setConfig(key:String, value:String){
    config.put( key, value )
  }
  
  def startTwitterStreaming( sc: SparkContext ){
    if ( ssc != null ){
      println("Twitter Stream already running");
      return;
    }
    
    if ( !validateConfiguration() ){
      return;
    }    
    
    ssc = new StreamingContext( sc, Seconds(20) )
    config.foreach( (t:(String,String)) => 
      if ( t._1.startsWith( "twitter4j") ) System.setProperty( t._1, t._2 )
    )

    val tweets = org.apache.spark.streaming.twitter.TwitterUtils.createStream( ssc, None )
    
    val keys = config.get("tweets.key");
    //val keys = Seq("#IBM", "#Cloudant", "#Bluemix", "#MTVHottest")
    tweets.foreachRDD( rdd => 
      rdd.foreachPartition { iterator => 
        lazy val client = PooledHttp1Client()
        val couch = CouchDb( config.get("cloudant.hostName").get, 
            config.get("cloudant.port").get.toInt, 
            config.get("cloudant.https").get.toBoolean, 
            config.get("cloudant.username").get, 
            config.get("cloudant.password").get);
        var t = couch.dbs.create("spark-streaming-twitter")
        t.attemptRun
        val typeMapping = TypeMapping(classOf[Tweet] -> "Tweet")
        val db = couch.db("spark-streaming-twitter", typeMapping)
        iterator.foreach( status => 
          if ( keys.isEmpty || keys.exists { status.getText().contains(_) } ) saveTweetToCloudant( client, db, status )
        )
      }
    )
   
    ssc.start()
    
    println("Twitter stream started");
  }
  
  //Class models for Sentiment JSON
  case class Sentiment( scorecard: String, children: Seq[Tone] )
  case class Tone( name: String, id: String, children: Seq[ToneResult])
  case class ToneResult(name: String, id: String, word_count: Double, normalized_score: Double, raw_score: Double, linguistic_evidence: Seq[LinguisticEvidence] )
  case class LinguisticEvidence( evidence_score: Double, word_count: Double, correlation: String, words : Seq[String])
  
  case class Geo( lat: Double, long: Double )
  case class Tweet(author: String, date: String, language: String, text: String, geo : Geo, sentiment : Sentiment )
  
  def saveTweetToCloudant(client: Client, db: CouchDbApi, status:Status){
    println("Calling sentiment from Watson Tone Analyzer: " + status.getText())
    
    //Get Sentiment on the tweet
    val sentimentResults: String = 
      EntityEncoder[String].toEntity("{\"text\": \"" + StringEscapeUtils.escapeJson( status.getText ) + "\"}" ).flatMap { 
      	entity =>
          val toneurl: String = config.get("watson.tone.url").get
        	client(
        			Request( 
        					method = Method.POST, 
        					uri = uri( toneurl ),
        					headers = Headers(
        							Authorization(
      									BasicCredentials(config.get("watson.tone.username").get, config.get("watson.tone.password").get)
    									),
    									Header("Accept", "application/json; charset=utf-8"),
    									Header("Content-Type", "application/json")
    							  ),
    							body = entity.body
    					)
    			).flatMap { response =>
             if (response.status.code == 200 ) {
              response.as[String]
             } else {
              println( "Error received from Watson Tone Analyzer: " + response.as[String] )
              null
            }
          }
      }.run
    println( sentimentResults)
    
    println("Creating new Tweet in Couch Database " + status.getText())
    try{
      val task = db.docs.create( 
          Tweet(
              status.getUser().getName, 
              status.getCreatedAt().toString(),
              status.getUser().getLang(),
              status.getText(),
              Geo( 
                  Option(status.getGeoLocation).map{ _.getLatitude}.getOrElse(0), 
                  Option(status.getGeoLocation).map{_.getLongitude}.getOrElse(0) 
              ),
              upickle.read[Sentiment]( sentimentResults )
          ) 
      )
      
      // Execute the actions and process the result
      task.attemptRun match {
        // In case of an error (left side of Either), print it
        case e => println(e)
      }
    }catch{
      case e: Exception => e.printStackTrace()
    }
  }
  
  def stopTwitterStreaming(){
    if ( ssc == null){
      println("No Twitter stream to stop");
      return;
    }
    
    ssc.stop();
    ssc = null;
    println("Twitter stream stopped");
  }
}