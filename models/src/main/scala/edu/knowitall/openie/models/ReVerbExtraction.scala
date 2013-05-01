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

@SerialVersionUID(7720340660222065636L)
case class ReVerbExtraction(
  val sentenceTokens: IndexedSeq[ChunkedToken],
  val arg1Interval: Interval,
  val relInterval:  Interval,
  val arg2Interval: Interval,
  val sourceUrl: String) extends Extraction {

  import ReVerbExtraction.{strippedDeterminers, modifierTagsToStrip, modifiersToKeep}

  override def sentenceText = sentenceTokens.map(_.string).mkString(" ")

  override def toString: String = ReVerbExtraction.serializeToString(this)

  private def writeReplace(): java.lang.Object = {
    new ReVerbExtractionSerializationProxy(this)
  }

  override def arg1Tokens = sentenceTokens(arg1Interval)

  override def relTokens = sentenceTokens(relInterval)

  override def arg2Tokens = sentenceTokens(arg2Interval)

  def normTokens(interval: Interval) = sentenceTokens(interval) filter indexTokenFilter map { token =>
    val stemmer = TaggedStemmer.instance
    val norm = stemmer.stem(token)
    new ChunkedToken(new PostaggedToken(new Token(norm, token.offset), token.postag), token.chunk)
  }

  def sentenceTokens(interval: Interval): Seq[ChunkedToken] = interval.map(sentenceTokens(_))

  def indexTokenFilter(token: Token) = !strippedDeterminers.contains(token.string.toLowerCase)

  // returns an (arg1, rel, arg2) tuple of normalized string tokens
  def indexGroupingKey: (String, String, String) = {

    val arg1Cleaned = arg1Tokens filter indexTokenFilter
    val relCleaned  =  relTokens filter indexTokenFilter
    val arg2Cleaned = arg2Tokens filter indexTokenFilter

    val stemmer = TaggedStemmer.instance

    val arg1Norm = stemmer.stemAll(arg1Cleaned)
    val relNorm =  stemmer.stemAll(relCleaned)
    val arg2Norm = stemmer.stemAll(arg2Cleaned)

    (arg1Norm.mkString(" ").toLowerCase, relNorm.mkString(" ").toLowerCase, arg2Norm.mkString(" ").toLowerCase)
  }

  def frontendGroupingKey: (String, String, String) = {

     (frontendArgKey(arg1Interval), frontendArgKey(relInterval), frontendArgKey(arg2Interval))
  }

  def arg1Head: String = {
    getHead(arg1Interval)
  }

  def arg2Head: String = {
    getHead(arg2Interval)
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

    val stemmer = TaggedStemmer.instance

    val stemmed = stemmer.stemAll(cleaned)

    stemmed.mkString(" ").toLowerCase
  }
}

object ReVerbExtraction extends TabSerializer[ReVerbExtraction] {

  private val tabSplitPattern = "\t".r
  private val spaceSplitPattern = "\\s".r
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

  override protected val tabDelimitedFormatSpec: List[(String, ReVerbExtraction => String)] = {
    type RVE = ReVerbExtraction
    List(
      ("arg1 range", (e: RVE) => e.arg1Interval.toString),
      ("rel range", (e: RVE) => e.relInterval.toString),
      ("arg2 range", (e: RVE) => e.arg2Interval.toString),
      ("sentence tokens", (e: RVE) => e.sentenceTokens.map(_.string).mkString(" ")),
      ("sentence postags", (e: RVE) => e.sentenceTokens.map(_.postag).mkString(" ")),
      ("sentence chunktags", (e: RVE) => e.sentenceTokens.map(_.chunk).mkString(" ")),
      ("source url", (e: RVE) => e.sourceUrl))
  }

  override def deserializeFromTokens(tokens: Seq[String]): Option[ReVerbExtraction] = {

    val split = tokens.take(tabDelimitedFormatSpec.length).toIndexedSeq

    def failure = { System.err.println("Unable to parse tab-delimited ReVerbExtraction from tokens: " + split); None }

    if (split.size != tabDelimitedFormatSpec.length) {
      failure
    } else {
      try {
        val argIntervals = split.take(3).flatMap(intervalFromString(_))
        if (argIntervals.size != 3) {
          failure
        } else {
          // here split.length is guaranteed to be right, and fieldRanges.length is guaranteed to be 3
          val sentLayers = split.drop(3).take(3).map(spaceSplitPattern.split(_))
          val sentenceTokens = chunkedTokensFromLayers(sentLayers(0), sentLayers(1), sentLayers(2))
          val sourceUrl = split(6)
          val extr = new ReVerbExtraction(sentenceTokens.toIndexedSeq, argIntervals(0), argIntervals(1), argIntervals(2), sourceUrl)
          Some(extr)
        }
      } catch {
        case e: Exception => {
          System.err.println("Exception parsing subsequent ReVerbExtraction")
          e.printStackTrace
          failure
        }
      }
    }
  }

  private def intervalFromString(str: String): Option[Interval] = {

    val endOpen = str.endsWith(")")

    val matches = for (s <- numExtractorPattern.findAllIn(str)) yield s
    matches toSeq match {
      case Seq(num1, num2) => {
        val start = num1.toInt
        val end = num2.toInt
        if (endOpen) Some(Interval.open(start, end))
        else Some(Interval.closed(start, end))
      }
      case Seq(num) => {
        Some(Interval.singleton(num.toInt))
      }
      case _ => { System.err.println("Couldn't parse interval:" + str); None }
    }
  }
}

@SerialVersionUID(1L)
private class ReVerbExtractionSerializationProxy(
  @transient var sentenceTokens: IndexedSeq[ChunkedToken],  // after seeing https://issues.scala-lang.org/browse/SI-5697, lets avoid serializing any scala objects.
  var arg1Interval: Interval,
  var relInterval:  Interval,
  var arg2Interval: Interval,
  var sourceUrl: String) extends scala.Serializable {

  def this() = this(IndexedSeq.empty, Interval.empty, Interval.empty, Interval.empty, "")

  def this(reVerbExtr: ReVerbExtraction) =
    this(reVerbExtr.sentenceTokens.toIndexedSeq, reVerbExtr.arg1Interval, reVerbExtr.relInterval, reVerbExtr.arg2Interval, reVerbExtr.sourceUrl)

  private def writeObject(oos: ObjectOutputStream): Unit = {

    oos.defaultWriteObject()

    // now serialize the chunkedTokens as a list of Tokens, then PosTags, then ChunkTags
    val numTokens = sentenceTokens.length
    val tokens = sentenceTokens.map(_.string)
    val posTags = sentenceTokens.map(_.postag)
    val chunkTags = sentenceTokens.map(_.chunk)

    // Write out number of sentence tokens
    oos.writeInt(numTokens)
    // write out sentence tokens
    tokens.foreach(token => oos.writeObject(token))
    // write out sentence pos tags
    posTags.foreach(posTag => oos.writeObject(posTag))
    // write out sentence chunk tags
    chunkTags.foreach(chkTag => oos.writeObject(chkTag))
  }

  private def readResolve(): java.lang.Object = {
    new ReVerbExtraction(this.sentenceTokens, this.arg1Interval, this.relInterval, this.arg2Interval, this.sourceUrl)
  }

  private def readObject(ois: ObjectInputStream): Unit = {

    ois.defaultReadObject()

    // read in number of sentence tokens
    val numTokens = ois.readInt()
    // read in tokens
    val tokens = for (_ <- 1 to numTokens) yield ois.readObject.asInstanceOf[String]
    // read in pos tags
    val posTags = for (_ <- 1 to numTokens) yield ois.readObject.asInstanceOf[String]
    // read in chunk tags
    val chunkTags = for (_ <- 1 to numTokens) yield ois.readObject.asInstanceOf[String]
    this.sentenceTokens = ReVerbExtraction.chunkedTokensFromLayers(tokens, posTags, chunkTags).toIndexedSeq
  }
}
