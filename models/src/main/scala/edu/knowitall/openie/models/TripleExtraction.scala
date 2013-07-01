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
import edu.knowitall.openie.models.serialize.TabFormat
import edu.knowitall.openie.models.serialize.SpecTabFormat
import scala.util.Try

@SerialVersionUID(7720340660222065636L)
case class TripleExtraction(
  val confidence: Double,
  val corpus: String,
  val sentenceTokens: Seq[ChunkedToken],
  override val arg1Text: String,
  override val relText: String,
  override val arg2Text: String,
  val arg1Interval: Interval,
  val relInterval: Interval,
  val arg2Interval: Interval,
  val source: String) extends Extraction {

  import TripleExtraction._

  override def sentenceText = sentenceTokens.map(_.string).mkString(" ")

  override def toString: String = implicitly[TabFormat[Extraction]].write(this)

  override def arg1Tokens = sentenceTokens(arg1Interval)
  override def relTokens = sentenceTokens(relInterval)
  override def arg2Tokens = sentenceTokens(arg2Interval)

  def normTokens(interval: Interval) = sentenceTokens(interval) filter indexTokenFilter map { token =>
    val norm = TaggedStemmer.stem(token)
    new ChunkedToken(new PostaggedToken(new Token(norm, token.offset), token.postag), token.chunk)
  }

  def sentenceTokens(interval: Interval): Seq[ChunkedToken] = interval.map(sentenceTokens(_))

  def indexTokenFilter(token: Token) = !Extraction.strippedDeterminers.contains(token.string.toLowerCase)

  // returns an (arg1, rel, arg2) tuple of normalized string tokens
  def indexGroupingKey: (String, String, String) = {

    val arg1Cleaned = arg1Tokens filter indexTokenFilter
    val relCleaned = relTokens filter indexTokenFilter
    val arg2Cleaned = arg2Tokens filter indexTokenFilter

    val arg1Norm = TaggedStemmer.stemAll(arg1Cleaned)
    val relNorm = TaggedStemmer.stemAll(relCleaned)
    val arg2Norm = TaggedStemmer.stemAll(arg2Cleaned)

    (arg1Norm.mkString(" ").toLowerCase, relNorm.mkString(" ").toLowerCase, arg2Norm.mkString(" ").toLowerCase)
  }

  def arg1Head: String = {
    getHead(arg1Interval)
  }

  def arg2Head: String = {
    getHead(arg2Interval)
  }

  private def getHead(argInterval: Interval): String = {
    val cleaned = sentenceTokens(argInterval).filter { token =>
      if (Extraction.strippedDeterminers.contains(token.string.toLowerCase)) false
      else if (Extraction.modifierTagsToStrip.contains(token.postag) && !Extraction.modifiersToKeep.contains(token.string.toLowerCase)) false
      else true
    }
    cleaned.mkString(" ");
  }

  private def frontendArgKey(argInterval: Interval): String = {
    // iterate over tokens. Strip them if they are:
    // 1. A stripped determiner
    // 2. A modifier: JJ, RB, VBG, $PRP, or WP that isn't also in modifiersToKeep.
    val cleaned = sentenceTokens(argInterval).filter { token =>
      if (Extraction.strippedDeterminers.contains(token.string.toLowerCase)) false
      else if (Extraction.modifierTagsToStrip.contains(token.postag) && !Extraction.modifiersToKeep.contains(token.string.toLowerCase)) false
      else true
    }

    val stemmed = TaggedStemmer.stemAll(cleaned)

    stemmed.mkString(" ").toLowerCase
  }
}