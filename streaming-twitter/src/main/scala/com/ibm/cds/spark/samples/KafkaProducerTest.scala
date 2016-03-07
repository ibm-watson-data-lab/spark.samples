package com.ibm.cds.spark.samples

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.ibm.cds.spark.samples.config.MessageHubConfig
import twitter4j.StallWarning
import twitter4j.Status
import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.TwitterStreamFactory
import scala.util.parsing.json.JSON
import java.io.InputStream
import twitter4j.TwitterStream
import com.ibm.cds.spark.samples.config.DemoConfig
import org.apache.spark.Logging


/**
 * @author dtaieb
 */
object KafkaProducerTest extends Logging{
  //Very verbose, enable only if necessary
  Logger.getLogger("org.apache.kafka").setLevel(Level.ALL)
  Logger.getLogger("kafka").setLevel(Level.ALL)
  
  var twitterStream : TwitterStream = _;
  
  def main(args: Array[String]): Unit = {
    createTwitterStream();
  }
  
  def createTwitterStream(props: DemoConfig=null):TwitterStream = {
    if( twitterStream != null){
      println("Twitter Stream already running. Please call closeTwitterStream first");
      return twitterStream;
    }
    var kafkaProps:MessageHubConfig = null;
    if ( props == null ){
      kafkaProps = new MessageHubConfig
    }else{
      kafkaProps = props.cloneConfig
    }
    kafkaProps.setValueSerializer[StatusSerializer]    
    kafkaProps.validateConfiguration("watson.tone.")
    kafkaProps.createTopicsIfNecessary( kafkaProps.getConfig(MessageHubConfig.KAFKA_TOPIC_TWEETS ) )
    val kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[java.lang.String, Status]( kafkaProps.toImmutableMap() );
    
    twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addListener( new StatusListener(){
      var lastSent:Long = 0;
      def onStatus(status: Status){
        if ( lastSent == 0 || System.currentTimeMillis() - lastSent > 200L){
          lastSent = System.currentTimeMillis()
          logInfo("Got a status  " + status.getText )
          val producerRecord = new ProducerRecord(kafkaProps.getConfig(MessageHubConfig.KAFKA_TOPIC_TWEETS ), "tweet", status )
          try{
            val metadata = kafkaProducer.send( producerRecord ).get(2000, TimeUnit.SECONDS);
            logInfo("Successfully sent record: Topic: " + metadata.topic + " Offset: " + metadata.offset )
          }catch{
            case e:Throwable => e.printStackTrace
          }
        }
      }
      def onDeletionNotice( notice: StatusDeletionNotice){
        
      }
      def onTrackLimitationNotice( numLimitation : Int){
        println("Received track limitation notice from Twitter: " + numLimitation)
      }
      
      def onException( e: Exception){
        println("Unexpected error from twitterStream: " + e.getMessage);
        logError(e.getMessage, e)
      }
      
      def onScrubGeo(lat: Long, long: Long ){
        
      }
      
      def onStallWarning(warning: StallWarning ){
        
      }     
    })
    
    //Start twitter stream sampling
    twitterStream.sample();
    
    println("Twitter stream started. Tweets will flow to MessageHub instance. Please call closeTwitterStream to stop the stream")
    twitterStream
  }
  
  def closeTwitterStream(){
    if ( twitterStream==null){
      println("Nothing to close. Twitter stream has not been started")
    }else{
      println("Stopping twitter stream");
      twitterStream.shutdown()
      twitterStream=null
      println("Twitter Stream stopped")
    }
  }
}

class StatusSerializer extends Serializer[Status]{
  def configure( props: java.util.Map[String, _], isKey: Boolean) = {
    
  }
  
  def close(){
    
  }
  
  def serialize(topic: String, value: Status ): Array[Byte] = {
    val baos = new ByteArrayOutputStream(1024)
    val oos = new ObjectOutputStream(baos)
    oos.writeObject( value )
    oos.close
    baos.toByteArray()
  }
}
object KafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    val kafkaProps = new MessageHubConfig
    val kafkaConsumer = new KafkaConsumer[java.lang.String, StatusAdapter](kafkaProps.toImmutableMap, new StringDeserializer(), new StatusDeserializer())
    
    kafkaConsumer.subscribe( List(kafkaProps.getConfig(MessageHubConfig.KAFKA_TOPIC_TWEETS )) )
    new Thread( new Runnable {
      def run(){
        while( true ){
          Thread.sleep( 1000L )
          val it = kafkaConsumer.poll(1000L).iterator
          while( it.hasNext() ){
            val record = it.next();
            println( record.value );
          }
        }
      }
    }).start
  }
}