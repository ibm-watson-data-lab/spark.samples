package com.ibm.cds.spark.samples.config

import scala.reflect.ClassTag
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import scala.collection.mutable.ListBuffer


/**
 * @author dtaieb
 */
class MessageHubConfig extends DemoConfig{  
  lazy val kafkaOptionKeys = ListBuffer[String]()
  override def initConfigKeys(){
    config = config ++ Map[String,String]( 
      registerConfigKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      registerConfigKey(CommonClientConfigs.CLIENT_ID_CONFIG),
      registerConfigKey("auto.offset.reset", "latest"),
      registerConfigKey("acks", "all"),
      registerConfigKey("retries", "0"),
      registerConfigKey("batch.size", "16384"),
      registerConfigKey("linger.ms", "1"),
      registerConfigKey("buffer.memory", "33554432"),
      registerConfigKey("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
      registerConfigKey("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
      registerConfigKey(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2"),
      registerConfigKey(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2"),
      registerConfigKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
      registerConfigKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
      registerConfigKey(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"),
      registerConfigKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL" ),
      
      registerConfigKey(MessageHubConfig.CHECKPOINT_DIR_KEY)
    )
  }

  override private[config] def registerConfigKey( key: String, default: String = null ) : (String,String) = {
    kafkaOptionKeys += key
    super.registerConfigKey(key,default)
  }
  
  def copyKafkaOptionKeys(other:MessageHubConfig){
    kafkaOptionKeys.foreach { key => other.setConfig(key, getConfig(key) ) }
  }
  
  def setValueSerializer[U]()(implicit c: ClassTag[U]){
    setConfig("value.serializer", c.runtimeClass.getName);
  }
  
  def setValueDeserializer[U]()(implicit c: ClassTag[U]){
    setConfig("value.deserializer", c.runtimeClass.getName);
  }
}

object MessageHubConfig{
  val CHECKPOINT_DIR_KEY = "checkpointDir"
}