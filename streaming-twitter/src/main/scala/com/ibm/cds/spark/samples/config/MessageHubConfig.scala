package com.ibm.cds.spark.samples.config

import org.apache.kafka.common.config.SSLConfigs
import org.apache.kafka.clients.CommonClientConfigs
import scala.reflect.ClassTag


/**
 * @author dtaieb
 */
class MessageHubConfig extends DemoConfig{
  
  override def initConfigKeys(){
    config = config ++ Map[String,String]( 
      registerConfigKey("bootstrap.servers"),
      registerConfigKey("client.id"),
      registerConfigKey("auto.offset.reset", "latest"),
      registerConfigKey("acks", "all"),
      registerConfigKey("retries", "0"),
      registerConfigKey("batch.size", "16384"),
      registerConfigKey("linger.ms", "1"),
      registerConfigKey("buffer.memory", "33554432"),
      registerConfigKey("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
      registerConfigKey("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
      registerConfigKey(SSLConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2"),
      registerConfigKey(SSLConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2"),
      registerConfigKey(SSLConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
      registerConfigKey(SSLConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
      registerConfigKey(SSLConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"),
      registerConfigKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL" )
    )
  }
  
  def setValueSerializer[U]()(implicit c: ClassTag[U]){
    setConfig("value.serializer", c.runtimeClass.getName);
  }
  
  def setValueDeserializer[U]()(implicit c: ClassTag[U]){
    setConfig("value.deserializer", c.runtimeClass.getName);
  }
}