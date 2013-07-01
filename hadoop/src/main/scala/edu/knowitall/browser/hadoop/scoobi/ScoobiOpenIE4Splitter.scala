package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import edu.knowitall.common.Timing
import edu.knowitall.collection.immutable.Interval
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
import edu.knowitall.srlie.SrlExtractor
import edu.knowitall.chunkedextractor.Relnoun
import edu.knowitall.tool.stem.MorphaStemmer
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.TripleExtraction
import edu.knowitall.srlie.confidence.SrlConfidenceFunction
import edu.knowitall.openie.models.NaryExtraction

object ScoobiOpenIE4Splitter extends ScoobiApp {

  private lazy val tabSplit = "\t".r

  def run(): Unit = {

    var inputPath, outputPath = ""
    var triples: Boolean = false

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
      opt("triples", "filter out triples", { triples = true })
    }

    if (!parser.parse(args)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    val output = lines.filter { line =>
      if (triples) line.startsWith("T\t")
      else line.startsWith("N\t")
    }.map { line =>
      line.substring(line.indexOf("\t") + 1)
    }

    persist(TextOutput.toTextFile(output, outputPath + "/"));
  }
}
