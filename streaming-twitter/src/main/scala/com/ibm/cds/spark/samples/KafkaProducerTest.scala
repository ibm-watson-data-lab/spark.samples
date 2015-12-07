package com.ibm.cds.spark.samples

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.Map
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import twitter4j.StallWarning
import twitter4j.Status
import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.TwitterStreamFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.ibm.cds.spark.samples.config.MessageHubConfig


/**
 * @author dtaieb
 */
object KafkaProducerTest {  
  val kafkaProps = new MessageHubConfig;
  kafkaProps.setValueSerializer[StatusSerializer]
  def main(args: Array[String]): Unit = {
    
    //Very verbose, enable only if necessary
    //Logger.getLogger("org.apache.kafka").setLevel(Level.ALL)
    //Logger.getLogger("kafka").setLevel(Level.ALL)
    
    kafkaProps.validateConfiguration("watson.tone.")
    
    val kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[java.lang.String, Status]( kafkaProps.toImmutableMap() );
    
    val twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addListener( new StatusListener(){
      var lastSent:Long = 0;
      def onStatus(status: Status){
        if ( lastSent == 0 || System.currentTimeMillis() - lastSent > 200L){
          lastSent = System.currentTimeMillis()
          println("Got a status  " + status.getText )
          val producerRecord = new ProducerRecord("demo.tweets.watson.topic", "tweet", status )
          val metadata = kafkaProducer.send( producerRecord ).get;
          println("Successfully sent record: Topic: " + metadata.topic + " Offset: " + metadata.offset )
        }
      }
      def onDeletionNotice( notice: StatusDeletionNotice){
        
      }
      def onTrackLimitationNotice( numLimitation : Int){
        
      }
      
      def onException( e: Exception){
        
      }
      
      def onScrubGeo(lat: Long, long: Long ){
        
      }
      
      def onStallWarning(warning: StallWarning ){
        
      }     
    })
    
    //Start twitter stream sampling
    twitterStream.sample();
  }
}

class StatusDeserializer extends Deserializer[Status]{
  def configure( props: Map[String, _], isKey: Boolean) = {
    
  }
  
  def close(){
    
  }
  
  def deserialize(topic: String, data: Array[Byte] ): Status = {
    val bais = new ByteArrayInputStream( data )
    val ois = new ObjectInputStream( bais )
    ois.close
    ois.readObject().asInstanceOf[Status]
  }
}

class StatusSerializer extends Serializer[Status]{
  def configure( props: Map[String, _], isKey: Boolean) = {
    
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
    val kafkaConsumer = new KafkaConsumer[java.lang.String, Status](KafkaProducerTest.kafkaProps.toImmutableMap, new StringDeserializer(), new StatusDeserializer())
    
    kafkaConsumer.subscribe( List("demo.tweets.watson.topic") )
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