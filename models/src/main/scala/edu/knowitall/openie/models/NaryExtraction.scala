package edu.knowitall.openie.models

import edu.knowitall.openie.models.serialize.TabSerializer
import java.net.URL
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.regex.Pattern
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import scala.collection.JavaConversions._
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.tool.chunk.ChunkedToken
import edu.knowitall.tool.postag.PostaggedToken
import edu.knowitall.tool.tokenize.Token
import edu.knowitall.common.HashCodeHelper
import edu.knowitall.openie.models.util.TaggedStemmer
import sjson.json.DefaultProtocol
import edu.knowitall.tool.tokenize.Tokenizer
import edu.knowitall.tool.chunk.Chunker
import scala.util.matching.Regex

case class NaryExtraction(
  val confidence: Double,
  val corpus: String,
  val sentenceTokens: Seq[ChunkedToken],
  val arg1: NaryExtraction.Part,
  val rel: NaryExtraction.Part,
  val arg2s: List[NaryExtraction.Part],
  val sourceUrl: String) {

  import NaryExtraction.Part
  import NaryExtraction.{strippedDeterminers, modifierTagsToStrip, modifiersToKeep}

  def sentenceText = sentenceTokens.map(_.string).mkString(" ")

  override def toString: String = NaryExtraction.serializeToString(this)

  def arg1Tokens = sentenceTokens(arg1.interval)
  def relTokens = sentenceTokens(rel.interval)
  def arg2sTokens = arg2s.map(arg2 => sentenceTokens(arg2.interval))

  def normTokens(interval: Interval) = sentenceTokens(interval) filter indexTokenFilter map { token =>
    val stemmer = TaggedStemmer
    val norm = stemmer.stem(token)
    new ChunkedToken(new PostaggedToken(new Token(norm, token.offset), token.postag), token.chunk)
  }

  def sentenceTokens(interval: Interval): Seq[ChunkedToken] = interval.map(sentenceTokens(_))

  def indexTokenFilter(token: Token) = !strippedDeterminers.contains(token.string.toLowerCase)

  def arg1Head: String = {
    getHead(arg1.interval)
  }

  def arg2Heads: List[String] = {
    arg2s.map(arg2 => getHead(arg2.interval))
  }

  private def getHead(argInterval: Interval): String = {
    val cleaned = sentenceTokens(argInterval).filter { token =>
      if (strippedDeterminers.contains(token.string.toLowerCase)) false
      else if (modifierTagsToStrip.contains(token.postag) && !modifiersToKeep.contains(token.string.toLowerCase)) false
      else true
    }
    cleaned.mkString(" ");
  }

  private def frontendArgKey(argInterval: Interval): String = {
    // iterate over tokens. Strip them if they are:
    // 1. A stripped determiner
    // 2. A modifier: JJ, RB, VBG, $PRP, or WP that isn't also in modifiersToKeep.
    val cleaned = sentenceTokens(argInterval).filter { token =>
      if (strippedDeterminers.contains(token.string.toLowerCase)) false
      else if (modifierTagsToStrip.contains(token.postag) && !modifiersToKeep.contains(token.string.toLowerCase)) false
      else true
    }

    val stemmer = TaggedStemmer

    val stemmed = stemmer.stemAll(cleaned)

    stemmed.mkString(" ").toLowerCase
  }
}

object NaryExtraction extends TabSerializer[NaryExtraction] {
  case class Part(text: String, interval: Interval)

  private val tabPattern = "\t".r
  private val spacePattern = "\\s".r
  private val numExtractorPattern = "([0-9]+)".r

  val strippedDeterminers = Set("a", "an", "the", "these", "those", "this", "that", "which", "what")

  val modifierTagsToStrip = Set("JJ", "JJR", "JJS", "RB", "RBR", "RBS", "VBG", "PRP$", "WDT", "WP")

  val modifiersToKeep = Set("n't", "not", "no", "as", "rarely", "never", "none", "ought", "would", "could", "should", "all")

  def chunkedTokensFromLayers(tokens: Seq[String], posTags: Seq[String], chunkTags: Seq[String]): Seq[ChunkedToken] = {
    var currentOffset = 0
    val offsets = for (token <- tokens) yield {
      val thisOffset = currentOffset;
      currentOffset += token.length + 1 // +1 for assumed trailing space
      thisOffset
    }
    tokens.zip(offsets).zip(posTags).zip(chunkTags).map { case (((token, offset), posTag), chunkTag) =>
      new ChunkedToken(string=token, postag=posTag, chunk=chunkTag, offset=offset)
    }
  }

  def tokensForInterval(interval: Interval, tokens: IndexedSeq[ChunkedToken]): Seq[ChunkedToken] = interval.map(tokens(_))

  override protected val tabDelimitedFormatSpec: List[(String, NaryExtraction => String)] = {
    type RVE = NaryExtraction
    List(
      ("confidence", (e: RVE) => e.confidence.toString),
      ("corpus", (e: RVE) => e.corpus.toString),
      ("arg1 text", (e: RVE) => e.arg1.text.toString),
      ("rel text", (e: RVE) => e.rel.text.toString),
      ("arg2 texts", (e: RVE) => e.arg2s.map(_.text.replaceAll(";", ",")).mkString("; ")),
      ("arg1 interval", (e: RVE) => e.arg1.interval.toString),
      ("rel interval", (e: RVE) => e.rel.interval.toString),
      ("arg2 intervals", (e: RVE) => e.arg2s.map(_.interval).mkString("; ")),
      ("sentence tokens", (e: RVE) => e.sentenceTokens.map(_.string).mkString(" ")),
      ("sentence postags", (e: RVE) => e.sentenceTokens.map(_.postag).mkString(" ")),
      ("sentence chunktags", (e: RVE) => e.sentenceTokens.map(_.chunk).mkString(" ")),
      ("source url", (e: RVE) => e.sourceUrl))
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

  override def deserializeFromTokens(tokens: Seq[String]): Option[NaryExtraction] = {

    val split = tokens.take(tabDelimitedFormatSpec.length)

    def onFailure() = System.err.println("Unable to parse tab-delimited NaryExtraction from tokens: " + split)

    if (split.size != tabDelimitedFormatSpec.length) {
      onFailure()
      None
    } else {
      try {
        val Seq(confidence, corpus,
            arg1Text, relText, arg2Texts,
            arg1Interval, relInterval, arg2Intervals,
            sentenceStrings, sentencePostags, sentenceChunks, sourceUrl) = split

        val arg1 = Part(arg1Text, deserializeInterval(arg1Interval))
        val rel = Part(relText, deserializeInterval(relInterval))
        val arg2s = (arg2Texts.split("; ") zip arg2Intervals.split("; ")).map { case (text, interval) =>
          Part(text, deserializeInterval(interval))
        }.toList

        val tokens = Tokenizer.computeOffsets(spacePattern.split(sentenceStrings), sentenceStrings)
        val sentenceTokens = Chunker.tokensFrom(spacePattern.split(sentenceChunks), spacePattern.split(sentencePostags), tokens)
        val extr = new NaryExtraction(confidence.toDouble, corpus, sentenceTokens,
            arg1, rel, arg2s,
            sourceUrl)
        Some(extr)
      } catch {
        case e: Exception => {
          e.printStackTrace
          onFailure()
          None
        }
      }
    }
  }
}