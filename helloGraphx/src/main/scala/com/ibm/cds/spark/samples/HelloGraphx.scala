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

import org.apache.spark._
import scalaz._
import java.net.URL
import java.util.Calendar
import java.net.URLEncoder
import java.text.SimpleDateFormat
import org.apache.spark.sql.SQLContext
import scala.collection.immutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.Row
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.http4s.EntityEncoder
import org.codehaus.jettison.json.JSONObject
import org.http4s.Uri
import org.http4s.Request
import org.http4s.BasicCredentials
import org.http4s.headers.Authorization
import org.http4s.Header
import org.http4s.Headers
import org.http4s.Method
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.client.Client
import org.http4s.EntityDecoder
import org.apache.spark.graphx.EdgeTriplet

class Node(val properties: Map[String, String]) extends Serializable
case class Airport(override val properties: Map[String,String]) extends Node(properties)
case class Country(override val properties: Map[String,String]) extends Node(properties)
case class Continent(override val properties: Map[String,String]) extends Node(properties)
case class Route(override val properties: Map[String, String]) extends Node(properties)

object HelloGraphx {
  
  //main method invoked when running as a standalone Spark Application
  def main(args: Array[String]) {
    lazy val client = PooledHttp1Client()
    val conf = new SparkConf().setAppName("Hello Graphx")
    val sc = new SparkContext(conf)

    println("Hello Graphx Demo. Load/Save a graph to/from Graphx RDDs")
    
    val sqlContext = new SQLContext(sc);
    
    //Load airports
    val airportsDF = sqlContext.read.format("com.databricks.spark.xml")
          .option("rowTag","node")
          .option("rootTag","graphml/graph")
          .load("/Users/dtaieb/Downloads/air-routes-graph/air-routes.graphml")
    airportsDF.printSchema()
    println(airportsDF.count())
    
    val airportsRdd: RDD[(VertexId, Node with Product)] = 
        airportsDF.map { x => {
          val propertiesMap:Map[String,String] = x.getAs[Seq[Row]]("data")
              .map { row => row.getAs[String]("@key")->row.getAs[String]("#VALUE") }.toMap
          val id = x.getAs[Long]("@id")
          val nodeType:String = propertiesMap.get("type").getOrElse("")
          nodeType match {
                case "airport" => (id, Airport(propertiesMap))
                case "country" => (id, Country(propertiesMap))
                case "continent" => (id, Continent(propertiesMap))
                case _ => println("Skip node with type " + nodeType); (id, null)
          }
        }}.filter( f => f._2 !=null )
    println(airportsRdd.take(5).deep.mkString("\n"))
    
    //Load routes
    val routesDF = sqlContext.read.format("com.databricks.spark.xml")
          .option("rowTag","edge")
          .option("rootTag","graphml/graph")
          .load("/Users/dtaieb/Downloads/air-routes-graph/air-routes.graphml")
    routesDF.printSchema()
    println(routesDF.count())
    
    val routesRdd: RDD[(Edge[Route])] = 
        routesDF.map { x => {
          val propertiesMap:Map[String,String] = x.getAs[Seq[Row]]("data")
              .map { row => row.getAs[String]("@key")->row.getAs[String]("#VALUE") }.toMap + 
              ("id" -> x.getAs[Long]("@id").toString)
          Edge(x.getAs[Long]("@source"), x.getAs[Long]("@target"),Route(propertiesMap))
        }}
    println(routesRdd.take(5).deep.mkString("\n"))
    
    val graph = Graph( airportsRdd, routesRdd )
    
    //Iterate over the graph and send the vertices/edges to Gremlin Server
    graph.triplets.foreach( f => {
      addTriplet(client, f );
    })    
    
    //Traverse all nodes and all vertices, send them to the graphdb service via gremlin
    sc.stop()
  }
  
  def escape(s:String):String={
    s.replace("'", "\\'")
  }
  
  def addTriplet(client: Client, f: EdgeTriplet[Node with Product, Route] ){
    val sb = new StringBuilder()

    //Add the source vertex if necessary
    sb.append( "v1=graph.traversal().V(" + f.srcId + ").tryNext().orElse(null);")
    sb.append(" if(!v1) v1=graph.addVertex(id, " + f.srcId)
    f.srcAttr.properties.foreach { case(k,v) => sb.append(",'" + escape(k) + "','" + escape(v) + "'" ) }
    sb.append(");")
    
    //Add the target vertex if necessary
    sb.append( "v2=graph.traversal().V(" + f.dstId + ").tryNext().orElse(null);")
    sb.append(" if(!v2) v2=graph.addVertex(id, " + f.dstId)
    f.dstAttr.properties.foreach { case(k,v) => sb.append(",'" + escape(k) + "','" + escape(v) + "'") }
    sb.append(");")
    
    //Add the edge
    sb.append("v1.addEdge('edge', v2")
    f.attr.properties.foreach { f => sb.append(",'" + escape(f._1) + "','" + escape(f._2) + "'") }
    sb.append(");")
    
    runScript(client, sb.toString )
  }
  
  def addVertex(client: Client, id: Long, keyValues: Seq[(String,String)]){
    val sb = new StringBuilder();
    sb.append( "if(!graph.traversal().V(" + id + ")) graph.addVertex(id, " + id);
    keyValues.foreach { case(k,v) => sb.append("," + k + "," + v) }
    sb.append(")")
    runScript(client, sb.toString() )
  }
  
  def runScript(client: Client, script: String){
    //println("{\"gremlin\":" + JSONObject.quote( script ) + "}")
      val results = EntityEncoder[String].toEntity("{\"gremlin\":" + JSONObject.quote( script ) + "}" ).flatMap { 
        entity => 
          val gremlinUri = Uri.fromString( "http://localhost:8182" ).getOrElse( null )
          client(
              Request( 
                  method = Method.POST, 
                  uri = gremlinUri,
                  headers = Headers(
                      Header("Accept", "application/json"),
                      Header("Content-Type", "application/json")
                    ),
                  body = entity.body
              )
          ).flatMap { response =>
             val res = response.as[String]
             if (response.status.code == 200 ) {              
              res
             } else {
              println( "Error received from Gremlin. Code : " + response.status.code + " reason: " + response.status.reason )
              res
            }
          }
      }.attemptRun match {
        case -\/(e) => //Ignore
        case \/-(a) => println(a)
      }
  }
}
