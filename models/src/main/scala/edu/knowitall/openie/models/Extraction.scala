package edu.knowitall.openie.models

import edu.knowitall.tool.postag.PostaggedToken
import sjson.json.DefaultProtocol
import sjson.json.Format

/**
  * A base class for all Extraction types.
  * Provides a common interface for the fields
  * of a binary extraction. Subclasses
  * should add functionality specific to their
  * particular kind of extraction.
  *
  * Subclasses should also include a companion object
  * capable of performing deserialization.
  *
  */
abstract class Extraction {

  def arg1Tokens: Seq[PostaggedToken]
  def relTokens: Seq[PostaggedToken]
  def arg2Tokens: Seq[PostaggedToken]

  def sentenceText: String

  def indexGroupingKey: (String, String, String)

  def indexGroupingKeyString: String = {
    val key = indexGroupingKey
    "%s__%s__%s".format(key._1, key._2, key._3)
  }

  def arg1Text = arg1Tokens.map(_.string).mkString(" ")
  def relText = relTokens.map(_.string).mkString(" ")
  def arg2Text = arg2Tokens.map(_.string).mkString(" ")
}


object ExtractionProtocol extends DefaultProtocol {
    import dispatch.classic.json._
    import sjson.json.JsonSerialization._

    implicit object ExtractionFormat extends Format[Extraction] {
      def reads(json: JsValue): Extraction = throw new IllegalStateException

      def writes(p: Extraction): JsValue =
        JsObject(List(
          (tojson("arg1Text").asInstanceOf[JsString], tojson(p.arg1Text)),
          (tojson("relText").asInstanceOf[JsString], tojson(p.relText)),
          (tojson("arg2Text").asInstanceOf[JsString], tojson(p.arg2Text)),
          (tojson("sentenceText").asInstanceOf[JsString], tojson(p.sentenceText))))
    }

    implicit object ReVerbExtractionFormat extends Format[ReVerbExtraction] {
      def reads(json: JsValue): ReVerbExtraction = throw new IllegalStateException

      def writes(p: ReVerbExtraction): JsValue =
        JsObject(List(
          (tojson("arg1Text").asInstanceOf[JsString], tojson(p.arg1Text)),
          (tojson("relText").asInstanceOf[JsString], tojson(p.relText)),
          (tojson("arg2Text").asInstanceOf[JsString], tojson(p.arg2Text)),
          (tojson("sentenceText").asInstanceOf[JsString], tojson(p.sentenceText))))
    }
}