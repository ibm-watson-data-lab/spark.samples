package com.ibm.cds.spark.samples

import scala.BigDecimal
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.immutable.Seq.canBuildFrom
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.reflect.ClassTag

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.http4s.client.blaze.PooledHttp1Client

import com.google.common.base.CharMatcher
import com.ibm.cds.spark.samples.config.MessageHubConfig
import com.ibm.cds.spark.samples.dstream.KafkaStreaming.KafkaStreamingContextAdapter

import twitter4j.Status

/**
 * @author dtaieb
 * Twitter+Watson sample app with MessageHub/Kafka
 */
object MessageHubStreamingTwitter {
  
  var ssc: StreamingContext = null
  var sqlContext: SQLContext = null
  var workingRDD: RDD[Row] = null
  
  val checkpointPathDir = "/Users/dtaieb/watsondev/temp/ssckafka";
  
  var toneScoresRDD:RDD[(String, (List[String], List[Double]))] = null;
  
  //Logger.getLogger("org.apache.kafka").setLevel(Level.ALL)
  //Logger.getLogger("kafka").setLevel(Level.ALL)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Streaming Twitter + Watson with MessageHub/Kafka Demo")
    val sc = new SparkContext(conf)
    startTwitterStreaming(sc);
  }
  
  case class EnrichedTweet( author:String, date: String, lang: String, text: String, lat: Double, long: Double, sentimentScores: Map[String, Double])
  
  def startTwitterStreaming( sc: SparkContext, stopAfter: Duration = Seconds(0) ){
    if ( ssc != null ){
      println("Twitter Stream already running");
      return;
    }
    
    workingRDD = sc.emptyRDD
    
    val kafkaProps = new MessageHubConfig;
    kafkaProps.setValueSerializer[StringSerializer];
    
    if ( !kafkaProps.validateConfiguration() ){
      return;
    }
    
    //Broadcast the config to each worker node
    val broadcastVar = sc.broadcast( kafkaProps.toImmutableMap )
    
    val kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[String, String]( kafkaProps.toImmutableMap );
    val queue = new scala.collection.mutable.Queue[(String, String)]  
    toneScoresRDD = sc.emptyRDD
    
    ssc = new StreamingContext( sc, Seconds(10) )
    ssc.checkpoint(checkpointPathDir);
//    ssc = StreamingContext.getOrCreate( 
//        checkpointPathDir, 
//        () => {
//          val ssc = new StreamingContext( sc, Seconds(10) )
//          ssc.checkpoint(checkpointPathDir);
//          ssc
//        }
//    );   
    
    new Thread( new Runnable() {
      def run(){
        while(ssc!=null){          
          queue.synchronized{
            while(!queue.isEmpty ){
              try{
                val task = queue.dequeue();
                val producerRecord = new ProducerRecord[String,String](task._1, "tweet", task._2 )
                val metadata = kafkaProducer.send( producerRecord ).get;
                println("Sent record " + metadata.offset() + " Topic " + task._1)
              }catch{
                case e:Throwable => e.printStackTrace()
              }
            }
            queue.wait();
          }
        }
      }
    },"Message Hub producer").start
    
    val stream = ssc.createKafkaStream[String, Status,StringDeserializer, StatusDeserializer](
      List("demo.tweets.watson.topic")
    );

    val keys = broadcastVar.value.get("tweets.key").get.split(",");
    val tweets = stream.map( t => t._2)
      .filter { status => 
        Option(status.getUser).flatMap[String] { 
          u => Option(u.getLang) 
        }.getOrElse("").startsWith("en") && CharMatcher.ASCII.matchesAllOf(status.getText) && ( keys.isEmpty || keys.exists{status.getText.contains(_)})
      }
    
    val rowTweets = tweets.map(status=> {
    	lazy val client = PooledHttp1Client()
      val sentiment = ToneAnalyzer.computeSentiment( client, status, broadcastVar )        
      var scoreMap : Map[String, Double] = Map()
      if ( sentiment != null ){
        for ( tone <- Option( sentiment.children ).getOrElse( Seq() ) ){
          for ( result <- Option( tone.children ).getOrElse( Seq() ) ){
            scoreMap.put( result.id, (BigDecimal(result.normalized_score).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) * 100.0 )
          }
        }
      }
      
      EnrichedTweet( 
          status.getUser.getName, 
          status.getCreatedAt.toString, 
          status.getUser.getLang, 
          status.getText, 
          Option(status.getGeoLocation).map{ _.getLatitude}.getOrElse(0.0),
          Option(status.getGeoLocation).map{_.getLongitude}.getOrElse(0.0),
          scoreMap
      )
    })
   
   val metricsStream = rowTweets.flatMap { eTweet => {
     val retList = ListBuffer[String]()
     for ( tag <- eTweet.text.split("\\s+") ){
       if ( tag.startsWith( "#") && tag.length > 1 ){
           for ( tone <- Option( eTweet.sentimentScores.keys ).getOrElse( Seq() ) ){
               retList += (tag + "-" + tone + ":" + eTweet.sentimentScores.getOrElse( tone, 0.0))
           }
       }
     }
     retList.toList
   }}
   .map { fullTag => {
       val split = fullTag.split(":");
       (split(0), split(1).toFloat) 
   }}
   .combineByKey( 
       (x:Float) => (x,1), 
       (x:(Float,Int), y:Float) => (x._1 + y, x._2+1), 
       (x:(Float,Int),y:(Float,Int)) => (x._1 + y._1, x._2 + y._2),
       new HashPartitioner(sc.defaultParallelism)
   )
   .map[(String,(Long/*count*/, List[(String, Double)]))]{ t => {
     val key = t._1;
     val ab = t._2;
     val split = key.split("-")
     (split(0), (ab._2, List((split(1), BigDecimal(ab._1/ab._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble ))))
   }}
   .reduceByKey( (t,u) => (t._1+u._1, (t._2 ::: u._2).sortWith( (l,r) => l._1.compareTo( r._1 ) < 0 )))
   .mapValues( (item:(Long, List[(String,Double)])) => {
     val unzip = item._2.unzip
     (item._1/(item._2.size), unzip._1, unzip._2)
   })
   .updateStateByKey( (a:scala.collection.Seq[(Long, List[String], List[Double])], b: Option[(Long, List[String], List[Double])]) => {
     val safeB = b.getOrElse( (0L, List(), List() ) )
     var listTones = safeB._2
     var listScores = safeB._3
     var count = safeB._1
     for( item <- a ){
       count += item._1
       listScores = listScores.zipAll( item._3, 0.0, 0.0).map{ case(a,b)=>a+b/2 }.toList
       listTones = item._2
     }
     
     Some( (count, listTones, listScores) )
   })
   
   metricsStream.print
   
   metricsStream.foreachRDD( rdd =>{
     val topHashTags = rdd.sortBy( f => f._2._1, false ).take(5)
     if ( !topHashTags.isEmpty){
    	 queue.synchronized{
         queue += (("topHashTags", TweetsMetricJsonSerializer.serialize(topHashTags.map( f => (f._1, f._2._1 )))))
    		 queue += (("topHashTags.toneScores", ToneScoreJsonSerializer.serialize(topHashTags)))
  				 try{
  					 queue.notify
  				 }catch{
  				   case e:Throwable=>e.printStackTrace();
  				 }
    	 }
     }
   })
   
    ssc.start
    
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

object TweetsMetricJsonSerializer{
  def serialize(value: Seq[(String,Long)] ): String = {   
    val sb = new StringBuilder("[")
    var comma = ""
    value.foreach( item => {
      sb.append( comma + "[\"" + item._1.replaceAll("\"", "") + "\"," + item._2 + "]")
      comma=","
    })
    sb.append("]")
    println("Serialized json: " + sb)
    sb.toString()
  }
}

object ToneScoreJsonSerializer{
  def serializeList[U:ClassTag]( label: String, value: List[U] ):String = {
    val sb = new StringBuilder("[\"" + label.replaceAll("\"", "") + "\"")
    value.foreach { item => {
      if ( item.isInstanceOf[String] ) {
        val s = ",\"" + item.toString().replaceAll("\"", "") + "\"";
        sb.append( s.replaceAll("\"\"", "\"") )
      }else if ( item.isInstanceOf[Double] ){
        sb.append("," + item )
      }
    }}
    sb.append("]")
    sb.toString
  }
  def serialize(value:Seq[(String, (Long, List[String], List[Double]))]):String={
    val sb = new StringBuilder("[")
    var comma = ""
    var appendToneData = true;
    value.foreach( item => {
      if ( appendToneData ){
        sb.append( comma + serializeList( "x", item._2._2 ) )
        appendToneData = false
        comma = ","
      }
      sb.append( comma + serializeList( item._1, item._2._3 ) )
      comma=","
    })
    sb.append("]")
    println("Serialized size: " + value.size + ". Tone json: " + sb)
    sb.toString()
  }
}