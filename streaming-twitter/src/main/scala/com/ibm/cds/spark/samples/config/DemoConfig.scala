package com.ibm.cds.spark.samples.config

import org.apache.kafka.clients.CommonClientConfigs
import java.io.FileInputStream
import java.io.InputStream
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext


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
      registerConfigKey("watson.api.version", "2016-02-11"),
      registerConfigKey("cloudant.save", "false" )
  )
  
  private def getKeyOrFail(key:String):String={
    config.get(key).getOrElse( {
      throw new IllegalStateException("Missing key: " + key)
    })
  }
  def set_hadoop_config(sc:SparkContext){
    val prefix = "fs.swift.service." + getKeyOrFail("name") 
    val hconf = sc.hadoopConfiguration
    hconf.set(prefix + ".auth.url", getKeyOrFail("auth_url")+"/v2.0/tokens")
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", getKeyOrFail("project_id"))
    hconf.set(prefix + ".username", getKeyOrFail("user_id"))
    hconf.set(prefix + ".password", getKeyOrFail("password"))
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", getKeyOrFail("region"))
    hconf.setBoolean(prefix + ".public", true)
  }
  
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
  
  private[config] def registerConfigKey( key: String, default: String = null ) : (String,String) = {
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
        try{
          if ( t._1.startsWith( "twitter4j") && t._2 != null && (ignorePrefix==null || !t._1.startsWith( ignorePrefix ) ) ) {
            System.setProperty( t._1, t._2.asInstanceOf[String] )
          }
        }catch{
          case e:Throwable => println("error" + t)
        }
      )
    }
    ret
  }
}

object DemoConfig extends DemoConfig
