package edu.knowitall.browser.hadoop.scoobi

import scala.collection.JavaConversions._
import com.nicta.scoobi.Scoobi._
import edu.knowitall.openie.models.ExtractionArgument
import edu.knowitall.openie.models.ExtractionRelation
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.washington.cs.knowitall.commonlib.Range
import edu.knowitall.collection.immutable.Interval
import edu.washington.cs.knowitall.extractor.conf.ReVerbOpenNlpConfFunction
import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction
import edu.washington.cs.knowitall.nlp.ChunkedSentence
import edu.knowitall.tool.chunk.ChunkedToken
import com.nicta.scoobi.io.text.TextOutput
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat

object GroupReGrouperStaticVars {
  val confLocal = new ThreadLocal[ReVerbOpenNlpConfFunction]() {
    override def initialValue = new ReVerbOpenNlpConfFunction()
  }
}

object ScoobiGroupReGrouper extends ScoobiApp {
  import GroupReGrouperStaticVars._

  val MAX_GROUP_SIZE = 1000

  private var extrsProcessed = 0

  private var groupsProcessed = 0

  def run() = {

    val (inputPath, outputPath) = (args(0), args(1))

    // serialized ReVerbExtractions
    val groups: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    val reGroups = groups.map { line =>
      val x = groupMapProcessor(line).get
      getKeyValuePair(x)
    }.groupByKey

    val combinedGroups = reGroups.map(keyValues => ReVerbExtractionGroup.serializeToString(combineGroups(keyValues._1, keyValues._2)))

    persist(TextOutput.toTextFile(combinedGroups, outputPath + "/"));
  }

  def combineGroups(key: String, groups: Iterable[String]): ExtractionGroup[ReVerbExtraction] = {

    val parsedGroups = groups.flatMap(str => ReVerbExtractionGroup.deserializeFromString(str))

    val allInstances = parsedGroups.flatMap { group =>
      val keyCheck = getKeyValuePair(group)._1
      if (!keyCheck.equals(key)) System.err.println("Key mismatch, found %s expected %s".format(keyCheck, key))
      group.instances
    }

    // if there are several groups, we want to be sure to use one with an entity attached. For now, we
    // assume that there would be at most one entity, so we can dodge the problem of what to do if
    // there were several.
    // Or if one group had an arg1 link and the other group had an arg2 link.

    val head = parsedGroups.find(group => group.arg1.hasEntity || group.arg2.hasEntity).getOrElse(parsedGroups.head)

    val combinedGroup = new ExtractionGroup(head.arg1, head.rel, head.arg2, allInstances.take(MAX_GROUP_SIZE).toSet)

    groupsProcessed += parsedGroups.size
    if (groupsProcessed % 10000 == 0) System.err.println("Groups combined: %d".format(groupsProcessed))

    combinedGroup
  }

  def getKeyValuePair(group: ExtractionGroup[ReVerbExtraction]): (String, String) = {

    extrsProcessed += 1
    if (extrsProcessed % 20000 == 0) System.err.println("Extractions processed: %d".format(extrsProcessed))

    (group.instances.head.extraction.indexGroupingKeyString, ReVerbExtractionGroup.serializeToString(group))
  }

  def groupMapProcessor(line: String): Option[ExtractionGroup[ReVerbExtraction]] = {

    // try to parse the line into a group
    val group = ReVerbExtractionGroup.deserializeFromString(line).getOrElse { return None }

    // go through and assign confs to any extractions without them
    val confedInstances = group.instances.map { inst => if (inst.confidence < 0) tryAddConf(inst) else inst } map removeControlChars

    val newGroup = new ExtractionGroup(removeCCsArg(group.arg1), removeCCsRel(group.rel), removeCCsArg(group.arg2), confedInstances)

    Some(newGroup)
  }

  /**
    * Tries to attach a conf to inst, if it doesn't already have one. If it fails, reports an error, but returns inst unchanged.
    */
  def tryAddConf(inst: Instance[ReVerbExtraction]): Instance[ReVerbExtraction] = {
    val cbe = extrToCBE(inst.extraction)
    try {

      val conf = confLocal.get().getConf(cbe)
      new Instance(inst.extraction, inst.corpus, conf)
    } catch {
      case e: Exception => { e.printStackTrace; System.err.println(cbe); System.err.println(inst); inst }
    }
  }

  def removeCCs(str: String): String = str.replaceAll("\t", " ").replaceAll("[\\p{C}]", "")
  def removeCCsArg(arg: ExtractionArgument) = arg.copy(norm = removeCCs(arg.norm))
  def removeCCsRel(rel: ExtractionRelation) = rel.copy(norm = removeCCs(rel.norm))

  def removeControlChars(inst: Instance[ReVerbExtraction]): Instance[ReVerbExtraction] = {

    val extr = inst.extraction
    val convertedSentence = extr.sentenceTokens.map(tok => new ChunkedToken(Symbol(tok.chunk), Symbol(tok.postag), removeCCs(tok.string), tok.offset))
    val convertedExtr = new ReVerbExtraction(convertedSentence, extr.arg1Interval, extr.relInterval, extr.arg2Interval, extr.sourceUrl)

    new Instance(convertedExtr, inst.corpus, inst.confidence)
  }

  def extrToCBE(extr: ReVerbExtraction): ChunkedBinaryExtraction = {

    implicit def intervalToRange(interval: Interval): Range = new Range(interval.start, interval.length)

    val sentToks = extr.sentenceTokens

    // make the chunked sentence
    val chunkedSentence = new ChunkedSentence(sentToks.map(_.string), sentToks.map(_.postag), sentToks.map(_.chunk))
    // convert intervals to ranges

    // build relation
    val rel = new ChunkedExtraction(chunkedSentence, extr.relInterval)
    // build args
    val arg1 = new ChunkedArgumentExtraction(chunkedSentence, extr.arg1Interval, rel)
    val arg2 = new ChunkedArgumentExtraction(chunkedSentence, extr.arg2Interval, rel)
    // build CBE
    val cbe = new ChunkedBinaryExtraction(rel, arg1, arg2)
    cbe
  }

}
