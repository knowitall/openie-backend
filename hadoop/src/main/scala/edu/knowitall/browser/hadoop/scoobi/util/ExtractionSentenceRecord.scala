package edu.knowitall.browser.hadoop.scoobi.util

/**
  * Created with IntelliJ IDEA.
  * User: niranjan
  * Date: 4/13/12
  * Time: 11:41 AM
  * To change this template use File | Settings | File Templates.
  */

import scala.collection.JavaConversions._
import java.io.{ DataOutput, DataInput }
import com.nicta.scoobi.core.WireFormat

class ExtractionSentenceRecord(val docname: String,
  val extractorType: String,
  val sentenceid: Int,
  var eid: Int,
  val url: String,
  val hashes: String,
  val confidence: Double,
  val arg1: String,
  val rel: String,
  val arg2: String,
  val norm1Arg1: String,
  val norm1Rel: String,
  val norm1Arg2: String,
  val norm2Arg1: String,
  val norm2Rel: String,
  val norm2Arg2: String,
  val norm1Arg1PosTags: String,
  val norm1RelPosTags: String,
  val norm1Arg2PosTags: String,
  val norm2Arg1PosTags: String,
  val norm2RelPosTags: String,
  val norm2Arg2PosTags: String,
  val arg1Tag: String,
  val relTag: String,
  val arg2Tag: String,
  val dataString: String) {

  val CHUNKTAGS_IDX = 3
  val POSTAGS_IDX = 2

  def chunkTags: scala.Seq[String] = dataString.split("\t")(CHUNKTAGS_IDX).trim.split(" ")

  def posTags: Seq[String] = dataString.split("\t")(POSTAGS_IDX).trim.split(" ")

  /**
    *
    *
    *
    * Based on the output of the ReVerbExtractorMapper.doMap
    *
    * new Text(record.docname),
    * new Text(record.sentenceid + "\t" + eid + "\t" + "NA" + "\t" + hashes + "\t"
    * norm1Arg1 + "\t" + norm1Rel + "\t" + norm1Arg2 + "\t"
    * + norm2Arg1 + "\t" + norm2Rel + "\t" + norm2Arg2 + "\t"
    * + record.dataString)
    *
    *
    * def tagString = arg1Tag + "\t" + re
    * @param splits
    * @return
    */
  def this(splits: Seq[String]) = this(splits(0), splits(1), splits(2).toInt, splits(3).toInt, splits(4), splits(5),
    splits(6).toDouble, splits(7), splits(8), splits(9), splits(10), splits(11), splits(12),
    splits(13), splits(14), splits(15), splits(16), splits(17),
    splits(18), splits(19), splits(20), splits(21), splits(22), splits(23), splits(24), splits.slice(25, splits.length).mkString("\t"))

  def this(text: String) = this(text.split("\t"))

  def norm1String = norm1Arg1 + "\t" + norm1Rel + "\t" + norm1Arg2

  def norm2String = norm2Arg1 + "\t" + norm2Rel + "\t" + norm2Arg2

  def tagString = arg1Tag + "\t" + relTag + "\t" + arg2Tag

  def sentence = dataString.split("\t")(0)
  //def metaString = docname + "\t" + sentenceid + "\t" + eid + "\t" + url + "\t" + hashes

  override def toString(): String = {

    return docname + '\t'
      extractorType + '\t'
      sentenceid.toString + '\t'
      eid.toString + '\t'
      url + '\t'
      hashes + '\t'
      confidence.toString + '\t'
      arg1 + '\t'
      rel + '\t'
      arg2 + '\t'
      norm1Arg1 + '\t'
      norm1Rel + '\t'
      norm1Arg2 + '\t'
      norm2Arg1 + '\t'
      norm2Rel + '\t'
      norm2Arg2 + '\t'
      norm1Arg1PosTags + '\t'
      norm1RelPosTags + '\t'
      norm1Arg2PosTags + '\t'
      norm2Arg1PosTags + '\t'
      norm2RelPosTags + '\t'
      norm2Arg2PosTags + '\t'
      arg1Tag + '\t'
      relTag + '\t'
      arg2Tag + '\t'
      dataString
  }

  implicit def ExtractionSentenceRecordFmt = new WireFormat[ExtractionSentenceRecord] {
    def toWire(eRecord: ExtractionSentenceRecord, out: DataOutput) = {
      out.writeBytes(eRecord.toString)
    }

    // Note the input is assumed to be in a single line.
    def fromWire(in: DataInput): ExtractionSentenceRecord = {
      new ExtractionSentenceRecord(in.readLine().split("\t"))
    }
    def show(eRecord: ExtractionSentenceRecord): String = eRecord.toString
  }

}
