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

import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted

/**
 * @author dtaieb
 */
class StreamingListener extends org.apache.spark.streaming.scheduler.StreamingListener {
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) { 
    println("Receiver Started: " + receiverStarted.receiverInfo.name )
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) { 
    println("Receiver Error: " + receiverError.receiverInfo.lastError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) { 
    println("Receiver Stopped: " + receiverStopped.receiverInfo.name)
    println("Reason: " + receiverStopped.receiverInfo.lastError + " : " + receiverStopped.receiverInfo.lastErrorMessage)
  }
  
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted){
    println("Batch started with " + batchStarted.batchInfo.numRecords + " records")
  }
  
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted){
    println("Batch completed with " + batchCompleted.batchInfo.numRecords + " records");
  }
}