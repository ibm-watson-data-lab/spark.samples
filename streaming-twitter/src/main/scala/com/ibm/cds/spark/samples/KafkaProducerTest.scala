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


/**
 * @author dtaieb
 */
object KafkaProducerTest {
  //Very verbose, enable only if necessary
  Logger.getLogger("org.apache.kafka").setLevel(Level.ALL)
  Logger.getLogger("kafka").setLevel(Level.ALL)
  
  val kafkaProps = new MessageHubConfig;
  kafkaProps.setValueSerializer[StatusSerializer]
  def main(args: Array[String]): Unit = {    
    kafkaProps.validateConfiguration("watson.tone.")
    val kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer[java.lang.String, Status]( kafkaProps.toImmutableMap() );
    
    val twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addListener( new StatusListener(){
      var lastSent:Long = 0;
      def onStatus(status: Status){
        if ( lastSent == 0 || System.currentTimeMillis() - lastSent > 200L){
          lastSent = System.currentTimeMillis()
          println("Got a status  " + status.getText )
          val producerRecord = new ProducerRecord(kafkaProps.getConfig(MessageHubConfig.KAFKA_TOPIC_TWEETS ), "tweet", status )
          try{
            val metadata = kafkaProducer.send( producerRecord ).get(2000, TimeUnit.SECONDS);
            println("Successfully sent record: Topic: " + metadata.topic + " Offset: " + metadata.offset )
          }catch{
            case e:Throwable => e.printStackTrace
          }
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
    val kafkaConsumer = new KafkaConsumer[java.lang.String, StatusAdapter](KafkaProducerTest.kafkaProps.toImmutableMap, new StringDeserializer(), new StatusDeserializer())
    
    kafkaConsumer.subscribe( List(KafkaProducerTest.kafkaProps.getConfig(MessageHubConfig.KAFKA_TOPIC_TWEETS )) )
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