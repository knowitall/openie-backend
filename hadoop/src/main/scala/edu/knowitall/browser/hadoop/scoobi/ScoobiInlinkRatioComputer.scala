package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.core.WireFormat

import com.nicta.scoobi.Scoobi._

import edu.knowitall.common.Timing._
import edu.knowitall.browser.extraction.ReVerbExtraction
import edu.knowitall.browser.extraction.FreeBaseEntity
import edu.knowitall.browser.extraction.FreeBaseType
import edu.knowitall.browser.extraction.Instance
import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.util.TaggedStemmer
import edu.knowitall.browser.extraction.ReVerbExtractionGroup

import scopt.OptionParser

object ScoobiInlinkRatioComputer extends ScoobiApp {

  private val NO_ENTITY = "*NO_ENTITY*"

  type REG = ExtractionGroup[ReVerbExtraction]

  def run(): Unit = {

    var inputPath, outputPath = ""
    var processArg1 = true

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups with inlink counts", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups with inlink counts", { str => outputPath = str })
      arg("processArg1", "arg1 for arg1, anything else for arg2", { str => processArg1 = str.equals("arg1") })
    }

    if (!parser.parse(args)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val argKeyValuePairs: DList[(String, String)] = lines.map { line =>
      val group = ReVerbExtractionGroup.deserializeFromString(line).get
      if (processArg1) {
        group.arg1.entity match {
          case Some(entity) => (entity.fbid, line)
          case None => ("*NO_ENTITY*", line)
        }
      } else {
        group.arg2.entity match {
          case Some(entity) => (entity.fbid, line)
          case None => ("*NO_ENTITY*", line)
        }
      }
    }

    val argGrouped = argKeyValuePairs.groupByKey

    val argsFinished = argGrouped.flatMap {
      case (key, extrGroups) =>
        if (!key.equals(NO_ENTITY)) Some(processReducerGroup(processArg1, extrGroups)) else None
    }

    persist(TextOutput.toTextFile(argsFinished, outputPath + "/"));
  }

  /**
    * Assumes all REGs are linked, don't call this if there isn't a link in given arg field.
    */
  def processReducerGroup(arg1: Boolean, rawExtrGroups: Iterable[String]): String = {

    val rawProcGroup = rawExtrGroups.head
    val procGroup = ReVerbExtractionGroup.deserializeFromString(rawProcGroup).get
    val procEntity = if (arg1) procGroup.arg1.entity.get else procGroup.arg2.entity.get
    def size = rawExtrGroups.size
    val inlinks = procEntity.inlinkRatio
    def ratio = size.toDouble / inlinks.toDouble

    "%.04f\t%s\t%s".format(ratio, procEntity.name, procEntity.fbid)
  }
}