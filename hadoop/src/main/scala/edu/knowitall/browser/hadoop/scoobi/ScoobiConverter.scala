package edu.knowitall.browser.hadoop.scoobi

import edu.knowitall.common.Timing
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.Instance
import edu.washington.cs.knowitall.extractor.ReVerbExtractor
import edu.washington.cs.knowitall.nlp.ChunkedSentence
import edu.washington.cs.knowitall.nlp.extraction.ChunkedBinaryExtraction
import edu.knowitall.tool.chunk.OpenNlpChunker
import edu.knowitall.tool.chunk.ChunkedToken
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
import edu.knowitall.openie.models.Extraction
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import com.nicta.scoobi.core.DList
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.openie.models.serialize.TabWriter

object ScoobiConverter extends ScoobiApp {

  implicit def rangeToInterval(range: edu.washington.cs.knowitall.commonlib.Range): Interval = Interval.closed(range.getStart, range.getLastIndex)

  private lazy val tabSplit = "\t".r
  private lazy val wsSplit = "\\s".r

  lazy val relnoun = new Relnoun
  lazy val srlie = new SrlExtractor
  lazy val srlieConf = SrlConfidenceFunction.loadDefaultClassifier()

  def run(): Unit = {

    var inputPath, outputPath, corpus = ""

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, sentences each on a line", { str => inputPath = str })
      arg("outputPath", "hdfs output path, chunked sentences", { str => outputPath = str })
      arg("corpus", "corpus name", { str => corpus = str })
    }

    if (!parser.parse(args)) return

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    val clusters = lines.map { line =>
      ReVerbExtractionGroup.deserializeFromTokens(line.split("\t")).map { reg =>
        val extractions = reg.instances.map { inst =>
          TripleExtraction(inst.confidence, inst.corpus, inst.extraction.sentenceTokens, inst.extraction.arg1Text, inst.extraction.relText, inst.extraction.arg2Text, inst.extraction.arg1Interval, inst.extraction.relInterval, inst.extraction.arg2Interval, inst.extraction.source)
        }.toSeq
        val cluster = ExtractionCluster[TripleExtraction](reg.arg1, reg.rel, reg.arg2, extractions)
        implicitly[TabWriter[ExtractionCluster[Extraction]]].write(cluster)
      }
    }

    persist(TextOutput.toTextFile(clusters, outputPath + "/"));
  }
}