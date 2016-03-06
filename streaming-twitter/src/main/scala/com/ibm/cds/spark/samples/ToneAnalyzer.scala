package com.ibm.cds.spark.samples

import org.http4s.EntityEncoder
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.Request
import org.http4s.BasicCredentials
import org.http4s.Header
import org.http4s.Headers
import org.http4s.Method
import org.http4s.headers.Authorization
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import scala.util.parsing.json.JSON
import org.codehaus.jettison.json.JSONObject

/**
 * @author dtaieb
 */

object ToneAnalyzer extends Logging{
  
  val sentimentFactors = Array(
      ("Anger","anger"),
      ("Disgust","disgust"),
      ("Fear","fear"),
      ("Joy","joy"),
      ("Sadness","sadness"),
      ("Analytical","analytical"),
      ("Confident","confident"),
      ("Tentative","tentative"),
      ("Openness","openness_big5"),
      ("Conscientiousness","conscientiousness_big5"),
      ("Extraversion","extraversion_big5"),
      ("Agreeableness","agreeableness_big5"),
      ("EmotionalRange","neuroticism_big5")
  )
  
  //Class models for Sentiment JSON
  case class DocumentTone( document_tone: Sentiment )
  case class Sentiment(tone_categories: Seq[ToneCategory]);
  case class ToneCategory(category_id: String, category_name: String, tones: Seq[Tone]);
  case class Tone(score: Double, tone_id: String, tone_name: String)
//  case class Sentiment( scorecard: String, children: Seq[Tone] )
//  case class Tone( name: String, id: String, children: Seq[ToneResult])
//  case class ToneResult(name: String, id: String, word_count: Double, normalized_score: Double, raw_score: Double, linguistic_evidence: Seq[LinguisticEvidence] )
//  case class LinguisticEvidence( evidence_score: Double, word_count: Double, correlation: String, words : Seq[String])
  
  case class Geo( lat: Double, long: Double )
  case class Tweet(author: String, date: String, language: String, text: String, geo : Geo, sentiment : Sentiment )
 
  def computeSentiment( client: Client, status:StatusAdapter, broadcastVar: Broadcast[Map[String,String]] ) : Sentiment = {
    logTrace("Calling sentiment from Watson Tone Analyzer: " + status.text)
    //Get Sentiment on the tweet
    val sentimentResults: String = 
      EntityEncoder[String].toEntity("{\"text\": " + JSONObject.quote( status.text ) + "}" ).flatMap { 
        entity =>
          val s = broadcastVar.value.get("watson.tone.url").get + "/v3/tone?version=" + broadcastVar.value.get("watson.api.version").get
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
              println( "Error received from Watson Tone Analyzer. Code : " + response.status.code + " reason: " + response.status.reason )
              null
            }
          }
      }.run
    try{
      upickle.read[DocumentTone](sentimentResults).document_tone
    }catch{
      case e:Throwable => {
        e.printStackTrace()
        null
      }
    }
  }
}