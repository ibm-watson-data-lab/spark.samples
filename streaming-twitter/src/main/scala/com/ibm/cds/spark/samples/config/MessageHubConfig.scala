package com.ibm.cds.spark.samples.config

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.JaasUtils
import scala.io.Source
import java.io.InputStream
import java.io.FileWriter
import java.io.File


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
      registerConfigKey("acks", "-1"),
      registerConfigKey("retries", "0"),
      registerConfigKey("batch.size", "16384"),
      registerConfigKey("linger.ms", "1"),
      registerConfigKey("buffer.memory", "33554432"),
      registerConfigKey("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
      registerConfigKey("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
      registerConfigKey(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2"),
      registerConfigKey(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2"),
      registerConfigKey(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS"),
      registerConfigKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getDefaultSSLTrustStoreLocation),
      registerConfigKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit"),
      registerConfigKey(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS"),
      registerConfigKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL" ),
      
      registerConfigKey(MessageHubConfig.CHECKPOINT_DIR_KEY),
      registerConfigKey(MessageHubConfig.KAFKA_TOPIC_TWEETS, "demo.tweets.watson.topic"),
      registerConfigKey(MessageHubConfig.KAFKA_USER_NAME),
      registerConfigKey(MessageHubConfig.KAFKA_USER_PASSWORD)
    )
    
    //Set the Jaas Config
    
  }
  
  private def getDefaultSSLTrustStoreLocation():String={
    "/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home/jre/lib/security/cacerts"
  }

  override private[config] def registerConfigKey( key: String, default: String = null ) : (String,String) = {
    kafkaOptionKeys += key
    super.registerConfigKey(key,default)
  }
  
  override def validateConfiguration(ignorePrefix:String=null) : Boolean = {
    val ret = super.validateConfiguration(ignorePrefix)
    if ( ret ){
      //Create the jaas configuration
      var is:InputStream = null
      try{
        is = MessageHubConfig.getClass.getClassLoader.getResourceAsStream("jaas.conf");
        val confString = Source.fromInputStream( is ).mkString
          .replace( "$USERNAME", getConfig(MessageHubConfig.KAFKA_USER_NAME ))
          .replace( "$PASSWORD", getConfig(MessageHubConfig.KAFKA_USER_PASSWORD) )
        println(confString)
        
        val confDir= new File( System.getProperty("java.io.tmpdir") + File.separator + 
            fixPath( getConfig(MessageHubConfig.KAFKA_USER_NAME) ) )// + File.separator + "jaas.conf"
        confDir.mkdirs
        val confFile = new File( confDir, "jaas.conf");
        val fw = new FileWriter( confFile );
        fw.write( confString )
        fw.close
        
        //Set the jaas login config property
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, confFile.getAbsolutePath );
      }finally{
        if ( is != null ) is.close
      }
    }
    ret
  }
  
  private def fixPath(path: String):String = {
    path.replaceAll("\\ / : * ? \" < > |,", "_")
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
  val KAFKA_TOPIC_TWEETS = "kafka.topic.tweet"    //Key for name of the kafka topic holding used for publishing the tweets
  val KAFKA_USER_NAME = "kafka.user.name"
  val KAFKA_USER_PASSWORD = "kafak.user.password"
}