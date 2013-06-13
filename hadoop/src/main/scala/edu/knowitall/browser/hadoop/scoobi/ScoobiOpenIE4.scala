package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import edu.knowitall.common.Timing
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.washington.cs.knowitall.extractor.ReVerbExtractor
import edu.washington.cs.knowitall.nlp.ChunkedSentence
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction
import edu.knowitall.tool.chunk.OpenNlpChunker
import edu.knowitall.tool.chunk.ChunkedToken
import scopt.OptionParser
import scala.collection.JavaConversions._
import com.nicta.scoobi.io.text.TextOutput
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat
import edu.knowitall.tool.parse.graph.DependencyGraph
import edu.knowitall.tool.chunk.Chunker
import edu.knowitall.tool.tokenize.Tokenizer

object ScoobiOpenIE4 extends ScoobiApp {

  implicit def rangeToInterval(range: edu.washington.cs.knowitall.commonlib.Range): Interval = Interval.closed(range.getStart, range.getLastIndex)

  private lazy val tabSplit = "\t".r
  private lazy val wsSplit = "\\s".r

  lazy val extractor = new ReVerbExtractor

  def run(): Unit = {

    var inputPath, outputPath = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
    }

    if (!parser.parse(args)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    def parseChunkedSentence(strs: Seq[String], poss: Seq[String], chks: Seq[String]): Option[ChunkedSentence] = {
      try {
        require(strs.length == poss.length && poss.length == chks.length)
        val chunkedSentence = new ChunkedSentence(strs, poss, chks)
        Some(chunkedSentence)
      } catch {
        case e: Exception => {
          System.err.println("Error parsing chunked sentence:\n%s\nStack trace:".format((strs, poss, chks)))
          e.printStackTrace
          System.err.println()
          None
        }
      }
    }

    def getExtractions(sentence: String, strs: Seq[String], poss: Seq[String], chks: Seq[String], dgraph: DependencyGraph, url: String): Iterable[ReVerbExtraction] = {
      val tokens = Chunker.tokensFrom(chks, poss, Tokenizer.computeOffsets(strs, sentence))
      val relnounExtrs = {
        val extrs = relnoun(tokens)
        Nil
      }

      val srlieExtrs {
        val extrs = srlie(dgraph)
        Nil
      }

      relnounExtrs ++ srlieExtrs
    }

    def split(str: String) = wsSplit.split(str)

    val finalExtractions = lines.flatMap { line =>
      tabSplit.split(line) match {
        case Array(_, url, _, _, sentence, strs, poss, chks, dependencies, _*) => {
          val rvExtrs = getExtractions(sentence, split(strs), split(poss), split(chks), DependencyGraph.deserialize(dependencies), url)
          rvExtrs map ReVerbExtraction.serializeToString
        }
        case _ => {
          System.err.println("Couldn't parse line: %s".format(line))
          Seq.empty
        }
      }
    }

    persist(TextOutput.toTextFile(finalExtractions, outputPath + "/"));
  }
}
