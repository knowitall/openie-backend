package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import java.io.File
import java.io.FileWriter
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.knowitall.common.Timing._
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.Pair
import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction
import scopt.OptionParser
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat

// A trivially simple utility for sampling lines from a file in Hadoop. Useful for creating debugging/test files for quickly
// iterating in hadoop.
object FileSampler extends ScoobiApp {

  def run(): Unit = {

    var inputPath = ""
    var outputPath = ""
    var sampleFrac = 1.0

    val parser = new OptionParser() {
      arg("inputPath", "file input path, records delimited by newlines", { str => inputPath = str })
      arg("outputPath", "file output path, newlines again", { str => outputPath = str })
      arg("sampleFrac", "sample size as a decimal in [0,1]", { str => sampleFrac = str.toDouble })
    }

    if (!parser.parse(args)) return

    println("Parsed args: %s".format(args.mkString(" ")))
    println("inputPath=%s".format(inputPath))
    println("outputPath=%s".format(outputPath))
    println("sampleFrac=%s".format(sampleFrac.toString))

    // serialized ReVerbExtractions
    val input: DList[String] = TextInput.fromTextSource(
      new TextSource(
        Seq(inputPath),
        inputFormat = classOf[LzoTextInputFormat]
      )
    )

    def sampler(line: String): Option[String] = if (Random.nextDouble <= sampleFrac) Some(line) else None

    val output = input flatMap sampler

    persist(toTextFile(output, outputPath + "/"))
  }
}
