package com.ibm.cds.spark.samples.config

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SSLConfigs
import java.io.FileInputStream
import java.io.InputStream
import scala.collection.JavaConversions._


/**
 * @author dtaieb
 */

class DemoConfig extends Serializable{  
  
  //Hold configuration key/value pairs
  var config = scala.collection.mutable.Map[String, String](
      registerConfigKey("twitter4j.oauth.consumerKey" ),
      registerConfigKey("twitter4j.oauth.consumerSecret" ),
      registerConfigKey("twitter4j.oauth.accessToken" ),
      registerConfigKey("twitter4j.oauth.accessTokenSecret"),
      registerConfigKey("tweets.key",""),
      registerConfigKey("cloudant.hostName" ),
      registerConfigKey("cloudant.https", "true"),
      registerConfigKey("cloudant.port" ),
      registerConfigKey("cloudant.username" ),
      registerConfigKey("cloudant.password" ),
      registerConfigKey("watson.tone.url" ),
      registerConfigKey("watson.tone.username" ),
      registerConfigKey("watson.tone.password" ),
      registerConfigKey("cloudant.save", "false" )
  )
  
  def initConfigKeys(){
    //Overridable by subclasses
  }
  
  //Give a chance to subclasses to init the keys
  initConfigKeys;
  
  {
    //Load config from property file if specified
    val configPath = System.getenv("DEMO_CONFIG_PATH");
    if ( configPath != null ){
      val props = new java.util.Properties
      var fis:InputStream = null
      try{
        fis = new FileInputStream(configPath)
        props.load(fis)
        for( key <- props.keysIterator ){
          setConfig( key, props.getProperty(key))
        }
      }catch{
        case e:Throwable => e.printStackTrace
      }finally{
        if ( fis != null ){
          fis.close
        }
      }    
    }
  }
  
  def registerConfigKey( key: String, default: String = null ) : (String,String) = {
    if ( default == null ){
      (key, Option(System.getProperty(key)).orNull )
    }
    (key, Option(System.getProperty(key)) getOrElse default )
  }
  
  def setConfig(key:String, value:String){
    config.put( key, value )
  }
  
  def getConfig(key:String):String={
    config.get(key).getOrElse("")
  }
  
  implicit def toImmutableMap(): Map[String,String]= {
    Map( config.toList: _* )
  }
  
  //Validate configuration settings
  def validateConfiguration(ignorePrefix:String=null) : Boolean = {
    var ret: Boolean = true;
    val saveToCloudant = config.get("cloudant.save").get.toBoolean
    config.foreach( (t:(String, Any)) => 
      if ( t._2 == null ){
        if ( saveToCloudant || !t._1.startsWith("cloudant")  ){
          if ( ignorePrefix == null || !t._1.startsWith( ignorePrefix )){
            println(t._1 + " configuration not set. Use setConfig(\"" + t._1 + "\",<your Value>)"); 
            ret = false;
          }
        }
      }
    )
    
    if ( ret ){
      config.foreach( (t:(String,Any)) => 
        if ( t._1.startsWith( "twitter4j") ) System.setProperty( t._1, t._2.asInstanceOf[String] )
      )
    }
    ret
  }
}

object DemoConfig extends DemoConfig
