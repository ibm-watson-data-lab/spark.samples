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

import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import org.apache.kafka.common.serialization.Serializer
import twitter4j.Status

/**
 * @author dtaieb
 */
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