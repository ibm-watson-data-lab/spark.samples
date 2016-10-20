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

import java.io.ObjectInputStream
import java.io.ByteArrayInputStream
import scala.util.parsing.json.JSON
import org.apache.kafka.common.serialization.Deserializer
import twitter4j.Status

/**
 * @author dtaieb
 * Deserialization adapters for Twitter4J Status
 */

case class StatusAdapter(userName:String, userId: String, userLang: String,createdAt:String,text:String, long:Double, lat:Double);

object StatusAdapter{
  implicit def statusAdapterWrapper(status: Status) = 
      StatusAdapter(
          status.getUser.getName,
          status.getUser.getScreenName,
          status.getUser.getLang,
          status.getCreatedAt.toString,
          status.getText,
          Option(status.getGeoLocation).map{ _.getLongitude}.getOrElse(0.0),
          Option(status.getGeoLocation).map{ _.getLatitude}.getOrElse(0.0)
      )
}

class StatusDeserializer extends Deserializer[StatusAdapter]{
  def configure( props: java.util.Map[String, _], isKey: Boolean) = {
    
  }
  
  def close(){
    
  }
  
  def deserialize(topic: String, data: Array[Byte] ): StatusAdapter = {
    try{
      val bais = new ByteArrayInputStream( data )
      var ois:ObjectInputStream = null
      try{
        ois = new ObjectInputStream( bais )
        ois.readObject().asInstanceOf[Status]
      }finally{
        if (bais != null ){
          bais.close
        }
        if ( ois != null ){
          ois.close
        }
      }
    }catch{
      case e:Throwable=>{
        val jsonObject = JSON.parseFull( new String(data) ).getOrElse(Map.empty).asInstanceOf[Map[String, Any]]
        val user=jsonObject.get("user").getOrElse( Map.empty ).asInstanceOf[Map[String,Any]]
        val geo = Option(jsonObject.get("geo").orNull).getOrElse(Map.empty).asInstanceOf[Map[String,Any]]
        StatusAdapter(
          user.get("name").getOrElse("").asInstanceOf[String], 
          user.get("userid").getOrElse("").asInstanceOf[String],
          user.get("lang").getOrElse("").asInstanceOf[String],
          jsonObject.get("created_at").getOrElse("").asInstanceOf[String],
          jsonObject.get("text").getOrElse("").asInstanceOf[String],
          geo.get("long").getOrElse(0.0).asInstanceOf[Double],
          geo.get("lat").getOrElse(0.0).asInstanceOf[Double]
        )
      }
    }
  }
}