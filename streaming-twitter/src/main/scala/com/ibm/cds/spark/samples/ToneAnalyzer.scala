package com.ibm.cds.spark.samples

import org.http4s.EntityEncoder
import org.http4s.Uri
import org.apache.commons.lang3.StringEscapeUtils
import org.http4s.client.Client
import twitter4j.Status
import org.http4s.Request
import org.http4s.BasicCredentials
import org.http4s.Header
import org.http4s.Headers
import org.http4s.Method
import org.http4s.headers.Authorization
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast

/**
 * @author dtaieb
 */

object ToneAnalyzer {
  
  val logger = Logger.getLogger( "com.ibm.cds.spark.samples.ToneAnalyzer" )
  
  val sentimentFactors = Array(
    ("Cheerfulness", "Cheerfulness" ), 
    ("Negative", "Negative"), 
    ("Anger", "Anger"), 
    ("Analytical", "Analytical"), 
    ("Confident", "Confident"), 
    ("Tentative", "Tentative"), 
    ("Openness", "Openness_Big5"), 
    ("Agreeableness", "Agreeableness_Big5"), 
    ("Conscientiousness", "Conscientiousness_Big5")
  )
  
  //Class models for Sentiment JSON
  case class Sentiment( scorecard: String, children: Seq[Tone] )
  case class Tone( name: String, id: String, children: Seq[ToneResult])
  case class ToneResult(name: String, id: String, word_count: Double, normalized_score: Double, raw_score: Double, linguistic_evidence: Seq[LinguisticEvidence] )
  case class LinguisticEvidence( evidence_score: Double, word_count: Double, correlation: String, words : Seq[String])
  
  case class Geo( lat: Double, long: Double )
  case class Tweet(author: String, date: String, language: String, text: String, geo : Geo, sentiment : Sentiment )
  
  def computeSentiment( client: Client, status:Status, broadcastVar: Broadcast[Map[String,String]] ) : Sentiment = {
    logger.trace("Calling sentiment from Watson Tone Analyzer: " + status.getText())
    //Get Sentiment on the tweet
    val sentimentResults: String = 
      EntityEncoder[String].toEntity("{\"text\": \"" + StringEscapeUtils.escapeJson( status.getText ) + "\"}" ).flatMap { 
        entity =>
          val s = broadcastVar.value.get("watson.tone.url").get + "/v1/tone"
          val toneuri: Uri = Uri.fromString( s ).getOrElse( null )
          client(
              Request( 
                  method = Method.POST, 
                  uri = toneuri,
                  headers = Headers(
                      Authorization(
                        BasicCredentials(broadcastVar.value.get("watson.tone.username").get, broadcastVar.value.get("watson.tone.password").get)
                      ),
                      Header("Accept", "application/json; charset=utf-8"),
                      Header("Content-Type", "application/json")
                    ),
                  body = entity.body
              )
          ).flatMap { response =>
             if (response.status.code == 200 ) {
              response.as[String]
             } else {
              println( "Error received from Watson Tone Analyzer: " + response.as[String] )
              null
            }
          }
      }.run
    upickle.read[Sentiment](sentimentResults)
  }
}