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

import scala.collection.mutable._
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Accumulator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.http4s._
import org.http4s.Http4s._
import org.http4s.Status._
import org.http4s.client.Client
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.headers.Authorization
import com.ibm.couchdb._
import scalaz._
import scalaz.concurrent.Task
import twitter4j.Status
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.EmptyRDD



/**
 * @author dtaieb
 */
object StreamingTwitter {
  var ssc: StreamingContext = null
  var sqlContext: SQLContext = null
  var workingRDD: RDD[Row] = null
  var schemaTweets : StructType = null
  val logger: Logger = Logger.getLogger( "com.ibm.cds.spark.samples.StreamingTwitter" )
  
  //main method invoked when running as a standalone Spark Application
  def main(args: Array[String]) {    
    val conf = new SparkConf().setAppName("Spark Streaming Twitter Demo")
    val sc = new SparkContext(conf)
    startTwitterStreaming(sc, Seconds(10));
  }
  
  //Hold configuration key/value pairs
  val config = Map[String, String](
      ("twitter4j.oauth.consumerKey", Option(System.getProperty("twitter4j.oauth.consumerKey")).orNull ),
      ("twitter4j.oauth.consumerSecret", Option(System.getProperty("twitter4j.oauth.consumerSecret")).orNull ),
      ("twitter4j.oauth.accessToken", Option(System.getProperty("twitter4j.oauth.accessToken")).orNull ),
      ("twitter4j.oauth.accessTokenSecret", Option(System.getProperty("twitter4j.oauth.accessTokenSecret")).orNull ),
      ("tweets.key", Option(System.getProperty("tweets.key")).getOrElse("")),
      ("cloudant.hostName", Option(System.getProperty("cloudant.hostName")).orNull ),
      ("cloudant.https", Option(System.getProperty("cloudant.https")).getOrElse( "true" ) ),
      ("cloudant.port", Option(System.getProperty("cloudant.port")).orNull ),
      ("cloudant.username", Option(System.getProperty("cloudant.username")).orNull ),
      ("cloudant.password", Option(System.getProperty("cloudant.password")).orNull ),
      ("watson.tone.url", Option(System.getProperty("watson.tone.url")).orNull ),
      ("watson.tone.username", Option(System.getProperty("watson.tone.username")).orNull ),
      ("watson.tone.password", Option(System.getProperty("watson.tone.password")).orNull ),
      ("cloudant.save", Option(System.getProperty("cloudant.save")).getOrElse("false") )
  )
  
  val sentimentFactors = Map[String, String](
    ("Cheerfulness", "Cheerfulness" ), 
    ("Negative", "Negative"), 
    ("Anger", "Anger"), 
    ("Analytical", "Analytical"), 
    ("Confident", "Confident"), 
    ("Tentative", "Tentative"), 
    ("Openness", "Openness_Big5"), 
    ("Agreeableness", "Agreeableness_Big5"), 
    ("Conscientiousness", "Conscientiousness_Big5")
  )
  
  //Validate configuration settings
  def validateConfiguration() : Boolean = {
    var ret: Boolean = true;
    var saveToCloudant = config.get("cloudant.save").get.toBoolean
    config.foreach( (t:(String, String)) => 
      if ( t._2 == null ){
        if ( saveToCloudant || !t._1.startsWith("cloudant") ){
          println(t._1 + " configuration not set. Use setConfig(\"" + t._1 + "\",<your Value>)"); 
          ret = false;
        }
      }
    )
    
    ret
  }
  
  def setConfig(key:String, value:String){
    config.put( key, value )
  }
  
  def startTwitterStreaming( sc: SparkContext, stopAfter: Duration = Seconds(0) ){
    if ( ssc != null ){
      println("Twitter Stream already running");
      return;
    }
    
    if ( !validateConfiguration() ){
      return;
    }
    
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    workingRDD = sc.emptyRDD
    
    ssc = new StreamingContext( sc, Seconds(5) )
    config.foreach( (t:(String,String)) => 
      if ( t._1.startsWith( "twitter4j") ) System.setProperty( t._1, t._2 )
    )
    
    sqlContext = new SQLContext(sc)
    val keys = config.get("tweets.key").get.split(",");
    var stream = org.apache.spark.streaming.twitter.TwitterUtils.createStream( ssc, None );
    
    if ( schemaTweets == null ){
      val schemaString = "author date lang text lat:int long:int"
      schemaTweets =
        StructType(
          schemaString.split(" ").map(
            fieldName => {
              val ar = fieldName.split(":")
              StructField(
                  ar.lift(0).get, 
                  ar.lift(1).getOrElse("string") match{
                    case "int" => IntegerType
                    case _ => StringType
                  },
                  true)
            }
          ).union( 
              sentimentFactors.map( f => StructField( f._1, DoubleType )).toArray[StructField]
          )
        )
    }
    val tweets = stream.filter { status => 
      Option(status.getUser).flatMap[String] { u => Option(u.getLang) }.getOrElse("").startsWith("en") && ( keys.isEmpty || keys.exists{status.getText.contains(_)})
    }
    
    val saveToCloudant = config.get("cloudant.save").get.toBoolean  
    lazy val client = PooledHttp1Client()
    val rowTweets = tweets.map(status=> {
      val sentiment = callToneAnalyzer(client, status)
      var scoreMap : Map[String, Double] = Map()
      for ( tone <- Option( sentiment.children ).getOrElse( Seq() ) ){
        for ( result <- Option( tone.children ).getOrElse( Seq() ) ){
          scoreMap.put( result.id, result.normalized_score )
        }
      }
      val colValues = List[Any](
        status.getUser.getName, //author
        status.getCreatedAt.toString,   //date
        status.getUser.getLang,  //Lang
        status.getText,               //text
        Option(status.getGeoLocation).map{ _.getLatitude}.getOrElse(0),      //lat
        Option(status.getGeoLocation).map{_.getLongitude}.getOrElse(0)    //long
      )
      colValues :+ sentimentFactors.mapValues { id => scoreMap.get( id ).getOrElse( 0 ) }
      //Return [Row, (sentiment, status)]
      (Row(colValues),(sentiment,status))
    })

    rowTweets.foreachRDD( rdd => {
      try{
        if ( rdd.count() > 0 ){
          workingRDD = sc.parallelize( rdd.map( t => t._1 ).collect()).union( workingRDD )
        }
      }catch{
          case e: Exception => e.printStackTrace()
      }

      if ( saveToCloudant ){
        rdd.foreachPartition { iterator => 
          var db: CouchDbApi = null;
          val couch = CouchDb( config.get("cloudant.hostName").get, 
              config.get("cloudant.port").get.toInt, 
              config.get("cloudant.https").get.toBoolean, 
              config.get("cloudant.username").get, 
              config.get("cloudant.password").get);
          var t = couch.dbs.create("spark-streaming-twitter")
          t.attemptRun
          val typeMapping = TypeMapping(classOf[Tweet] -> "Tweet")
          db = couch.db("spark-streaming-twitter", typeMapping)
          iterator.foreach( t => {
              saveTweetToCloudant( client, db, t._2._2, t._2._1 )
            }
          )
        }
      }

    })
   
    val workingTweets = tweets.window( Seconds(10) )
    ssc.start()
    
    println("Twitter stream started");
    println("Tweets are collected real-time and analyzed")
    println("To stop the streaming and start interacting with the data use: StreamingTwitter.stopTwitterStreaming")
    
    if ( !stopAfter.isZero ){
      //Automatically stop it after 10s
      new Thread( new Runnable {
        def run(){
          Thread.sleep( stopAfter.milliseconds )
          stopTwitterStreaming
        }
      }).start
    }
  }
  
  //Class models for Sentiment JSON
  case class Sentiment( scorecard: String, children: Seq[Tone] )
  case class Tone( name: String, id: String, children: Seq[ToneResult])
  case class ToneResult(name: String, id: String, word_count: Double, normalized_score: Double, raw_score: Double, linguistic_evidence: Seq[LinguisticEvidence] )
  case class LinguisticEvidence( evidence_score: Double, word_count: Double, correlation: String, words : Seq[String])
  
  case class Geo( lat: Double, long: Double )
  case class Tweet(author: String, date: String, language: String, text: String, geo : Geo, sentiment : Sentiment )
  
  def callToneAnalyzer( client: Client, status:Status ) : Sentiment = {
    logger.trace("Calling sentiment from Watson Tone Analyzer: " + status.getText())
    //Get Sentiment on the tweet
    val sentimentResults: String = 
      EntityEncoder[String].toEntity("{\"text\": \"" + StringEscapeUtils.escapeJson( status.getText ) + "\"}" ).flatMap { 
        entity =>
          val s = config.get("watson.tone.url").get + "/v1/tone"
          val toneuri: Uri = Uri.fromString( s ).getOrElse( null )
          client(
              Request( 
                  method = Method.POST, 
                  uri = toneuri,
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
    upickle.read[Sentiment](sentimentResults)
  }
  def saveTweetToCloudant(client: Client, db: CouchDbApi, status:Status, sentiment: Sentiment) : Status = {    
    if ( db != null){
      logger.trace("Creating new Tweet in Couch Database " + status.getText())
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
                sentiment
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
      
    status
  }
  
  def createTwitterDataFrames(sc: SparkContext) : (SQLContext, DataFrame) = {
    if ( workingRDD.count <= 0 ){
      println("No data receive. Please start the Twitter stream again to collect data")
      return null
    }
    
    try{
      val df = sqlContext.createDataFrame( workingRDD, schemaTweets )
      df.registerTempTable("tweets")
      
      println("A new table named tweets with " + df.count() + " records has been correctly created and can be accessed through the SQLContext variable")
      println("Here's the schema for tweets")
      df.printSchema()
      
      (sqlContext, df)
    }catch{
      case e: Exception => {e.printStackTrace(); return null}
    }
  }
 
  def stopTwitterStreaming(){
    if ( ssc == null){
      println("No Twitter stream to stop");
      return;
    }
    
    println("Stopping Twitter stream. Please wait this may take a while")
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    ssc = null
    println("Twitter stream stopped");
    
    println( "You can now create a sqlContext and DataFrame with " + workingRDD.count + " Tweets created. Sample usage: ")
    println("val (sqlContext, df) = com.ibm.cds.spark.samples.StreamingTwitter.createTwitterDataFrames(sc)")
    println("df.printSchema")
    println("sqlContext.sql(\"select author, text from tweets\").show")
  }
}