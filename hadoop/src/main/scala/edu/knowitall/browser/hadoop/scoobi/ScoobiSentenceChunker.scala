package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._

import edu.knowitall.common.Timing._
import edu.knowitall.browser.extraction.ReVerbExtraction
import edu.knowitall.browser.extraction.FreeBaseEntity
import edu.knowitall.browser.extraction.FreeBaseType
import edu.knowitall.browser.extraction.Instance
import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.util.TaggedStemmer
import edu.knowitall.browser.extraction.ReVerbExtractionGroup

import edu.knowitall.tool.chunk.OpenNlpChunker
import edu.knowitall.tool.chunk.ChunkedToken

import scopt.OptionParser

object ScoobiSentenceChunker extends ScoobiApp {

  lazy val chunker = new OpenNlpChunker

  def run(): Unit = {

    var inputPath, outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
    }

    if (!parser.parse(args)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    def sentenceToTriple(toks: Seq[ChunkedToken], url: String): String = {
      val strs = toks.map(_.string.trim).mkString(" ")
      val poss = toks.map(_.postag.trim).mkString(" ")
      val chks = toks.map(_.chunk.trim).mkString(" ")

      Seq(strs, poss, chks, url).mkString("\t")
    }

    val output = lines.flatMap { line =>
      line.split("\t") match {
        case Array(rawSent, url, _*) => {
          val toks = chunker.chunk(rawSent)
          val result = sentenceToTriple(toks, url)
          Some(result)
        }
        case _ => None
      }
    }

    persist(TextOutput.toTextFile(output, outputPath + "/"));
  }
}