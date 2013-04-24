package edu.knowitall.browser.hadoop.scoobi

import scala.Option.option2Iterable

import com.nicta.scoobi.Scoobi.DList
import com.nicta.scoobi.Scoobi.ScoobiApp
import com.nicta.scoobi.Scoobi.StringFmt
import com.nicta.scoobi.Scoobi.TextInput
import com.nicta.scoobi.Scoobi.TextOutput
import com.nicta.scoobi.Scoobi.persist

import edu.knowitall.openie.models.ReVerbExtractionGroup
import scopt.OptionParser

/**
  * A mapper job that takes tab-delimited ReVerbExtractions as input, and outputs the mapping
  * between the tuple args and the tuple args' head phrase.
  */
object ScoobiHeadPhraseComparer extends ScoobiApp {
  def getHeadWords(groups:DList[String]): DList[String] = {
    groups.flatMap { line =>
      ReVerbExtractionGroup.deserializeFromString(line) match {
        case Some(extractionGroup) => {
          val firstExtraction = extractionGroup.instances.head.extraction
          List(
            "%s\t%s".format(extractionGroup.arg1.norm, firstExtraction.arg1Head),
            "%s\t%s".format(extractionGroup.arg2.norm, firstExtraction.arg2Head)
          )
        }
        case None => {
          System.err.println("Error parsing a group: %s".format(line));
          None
        }
      }
    }
  }

  def run() = {
    var inputPath, outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups", { str => outputPath = str })
    }

    if (parser.parse(args)) {
      val lines: DList[String] = TextInput.fromTextFile(inputPath)
      val headWords: DList[String] = getHeadWords(lines)
      persist(TextOutput.toTextFile(headWords, outputPath + "/"));
    }
  }
}
