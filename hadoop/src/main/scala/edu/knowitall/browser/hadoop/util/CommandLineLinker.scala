package edu.knowitall.browser.hadoop.util

import scopt.OptionParser
import scala.io.Source
import java.io.PrintStream
import java.io.BufferedReader
import java.io.InputStreamReader

import edu.knowitall.common.Resource.using
import edu.knowitall.common.Timing
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction

import edu.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker

class CommandLineLinker(val source: Source, val output: PrintStream) {
  private val tabSplit = "\t".r

  type REG = ExtractionGroup[ReVerbExtraction]

  def linkData: Unit = {

    def toGroups(line: String): Option[REG] = {
      ReVerbExtractionGroup.deserializeFromString(line)
    }

    var groupsProcessed = 0

    val runtimeNanos = Timing.time {
      val linker = ScoobiEntityLinker.getEntityLinker(1)
      // convert input lines to REGs
      val groups = source.getLines flatMap (line => { groupsProcessed += 1; toGroups(line) })
      val linkedGroups = groups map linker.linkEntities(reuseLinks = false)
      val strings = linkedGroups map ReVerbExtractionGroup.serializeToString
      strings foreach (output.println(_))
    }

    val runtimeString = Timing.Seconds.format(runtimeNanos)
    val groupsProcString = groupsProcessed.toString
    val msecPerGroup = Timing.Milliseconds.format(runtimeNanos / groupsProcessed)

    System.err.println("Running time: %s sec, Groups processed: %s, %s msec/group".format(runtimeString, groupsProcString, msecPerGroup))
  }
}

/** reads REGs from standard input. Writes them back to standard output. */
object CommandLineLinker {

  def main(args: Array[String]): Unit = {

    //var basePath = ScoobiEntityLinker.baseIndex
    var inputFile = ""
    var outputFile = ""

    val parser = new OptionParser() {
      opt("inputFile", "File to read for extractionGroup input, default standard input", { str => inputFile = str })
      opt("outputFile", "File to write linked ExtractionGroups to, default stdouts", { str => outputFile = str })
      //opt("basePath", "Base path for linker support files", { str => basePath = str })
    }

    if (!parser.parse(args)) return

    val source = if (!inputFile.isEmpty) Source.fromFile(inputFile) else Source.fromInputStream(System.in)

    val output = if (!outputFile.isEmpty) new java.io.PrintStream(new java.io.FileOutputStream(outputFile)) else System.out

    new CommandLineLinker(source, output).linkData

    source.close()
    output.close()
  }
}