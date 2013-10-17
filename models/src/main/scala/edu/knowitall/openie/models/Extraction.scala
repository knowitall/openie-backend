package edu.knowitall.openie.models

import edu.knowitall.tool.postag.PostaggedToken
import sjson.json.DefaultProtocol
import sjson.json.Format
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.openie.models.util.TaggedStemmer
import edu.knowitall.openie.models.serialize.TabFormat
import edu.knowitall.openie.models.serialize.SpecTabFormat
import edu.knowitall.tool.chunk.ChunkedToken
import scala.util.matching.Regex
import scala.util.Try
import edu.knowitall.tool.tokenize.Tokenizer
import edu.knowitall.tool.chunk.Chunker
import edu.knowitall.tool.tokenize.Token

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
  def arg1Tokens: Seq[ChunkedToken]
  def relTokens: Seq[ChunkedToken]
  def arg2Tokens: Seq[ChunkedToken]

  def arg1Interval: Interval
  def relInterval: Interval
  def arg2Interval: Interval

  def sentenceText: String
  def sentenceTokens: Seq[ChunkedToken]

  def indexGroupingKey: (String, String, String)

  def indexGroupingKeyString: String = {
    val key = indexGroupingKey
    "%s__%s__%s".format(key._1, key._2, key._3)
  }

  def arg1Text = arg1Tokens.map(_.string).mkString(" ")
  def relText = relTokens.map(_.string).mkString(" ")
  def arg2Text = arg2Tokens.map(_.string).mkString(" ")

  def confidence: Double
  def corpus: String
  def source: String

  import ReVerbExtraction.strippedDeterminers
  def indexTokenFilter(token: Token) = !strippedDeterminers.contains(token.string.toLowerCase)
  def stemmedTokens(tokens: Seq[ChunkedToken]) = tokens filter indexTokenFilter map { token =>
    val norm = TaggedStemmer.stem(token)
    new ChunkedToken(Symbol(token.chunk), Symbol(token.postag), norm, token.offset)
  }

  def frontendGroupingKey: (String, String, String) = {
    (frontendArgKey(arg1Tokens), frontendArgKey(relTokens), frontendArgKey(arg2Tokens))
  }
  private def frontendArgKey(tokens: Seq[PostaggedToken]): String = {
    // iterate over tokens. Strip them if they are:
    // 1. A stripped determiner
    // 2. A modifier: JJ, RB, VBG, $PRP, or WP that isn't also in modifiersToKeep.
    val cleaned = tokens.filter { token =>
      if (Extraction.strippedDeterminers.contains(token.string.toLowerCase)) false
      else if (Extraction.modifierTagsToStrip.contains(token.postag) && !Extraction.modifiersToKeep.contains(token.string.toLowerCase)) false
      else true
    }

    val stemmed = TaggedStemmer.stemAll(cleaned)

    stemmed.mkString(" ").toLowerCase
  }
}

object Extraction {
  val strippedDeterminers = Set("a", "an", "the", "these", "those", "this", "that", "which", "what")
  val modifierTagsToStrip = Set("JJ", "JJR", "JJS", "RB", "RBR", "RBS", "VBG", "PRP$", "WDT", "WP")
  val modifiersToKeep = Set("n't", "not", "no", "as", "rarely", "never", "none", "ought", "would", "could", "should", "all")

  implicit def formatter: SpecTabFormat[Extraction] = TabFormat

  object TabFormat extends SpecTabFormat[Extraction] {
    private val spacePattern = "\\s".r
    private val numExtractorPattern = "([0-9]+)".r

    def tokensForInterval(interval: Interval, tokens: IndexedSeq[ChunkedToken]): Seq[ChunkedToken] = interval.map(tokens(_))

    override val spec: List[(String, Extraction => String)] = {
      List(
        ("confidence", (e: Extraction) => e.confidence.toString),
        ("corpus", (e: Extraction) => e.corpus.toString),
        ("arg1 text", (e: Extraction) => e.arg1Text.toString),
        ("rel text", (e: Extraction) => e.relText.toString),
        ("arg2 text", (e: Extraction) => e.arg2Text.toString),
        ("arg1 interval", (e: Extraction) => e.arg1Interval.toString),
        ("rel interval", (e: Extraction) => e.relInterval.toString),
        ("arg2 interval", (e: Extraction) => e.arg2Interval.toString),
        ("sentence tokens", (e: Extraction) => e.sentenceTokens.map(_.string).mkString(" ")),
        ("sentence postags", (e: Extraction) => e.sentenceTokens.map(_.postag).mkString(" ")),
        ("sentence chunktags", (e: Extraction) => e.sentenceTokens.map(_.chunk).mkString(" ")),
        ("source url", (e: Extraction) => e.source))
    }

    val emptyRegex = new Regex("\\{\\}")
    val singletonRegex = new Regex("\\{([+-]?\\d+)\\}")
    val openIntervalRegex = new Regex("\\[([+-]?\\d+), ([+-]?\\d+)\\)")
    val closedIntervalRegex = new Regex("\\[([+-]?\\d+), ([+-]?\\d+)\\]")
    def deserializeInterval(pickled: String) = {
      pickled match {
        case emptyRegex() => Interval.empty
        case singletonRegex(value) => Interval.singleton(value.toInt)
        case openIntervalRegex(a, b) => Interval.open(a.toInt, b.toInt)
        case closedIntervalRegex(a, b) => Interval.closed(a.toInt, b.toInt)
      }
    }

    override def readSeq(tokens: Seq[String]): Try[TripleExtraction] = {
      val split = tokens

      Try {
        val Seq(confidence, corpus,
          arg1Text, relText, arg2Text,
          arg1Interval, relInterval, arg2Interval,
          sentenceStrings, sentencePostags, sentenceChunks, sourceUrl) = split

        val tokens = Tokenizer.computeOffsets(spacePattern.split(sentenceStrings), sentenceStrings)
        val sentenceTokens = Chunker.tokensFrom(spacePattern.split(sentenceChunks), spacePattern.split(sentencePostags), tokens)
        val extr = new TripleExtraction(confidence.toDouble, corpus, sentenceTokens,
          arg1Text, relText, arg2Text,
          deserializeInterval(arg1Interval), deserializeInterval(relInterval), deserializeInterval(arg2Interval),
          sourceUrl)
        extr
      }
    }
  }
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