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

object ScoobiOpenIE4 extends ScoobiApp {

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

    def getExtractions(sentence: String, strs: Seq[String], poss: Seq[String], chks: Seq[String], dgraph: DependencyGraph, url: String): (Iterable[TripleExtraction], Iterable[NaryExtraction]) = {
      val tokens = Chunker.tokensFrom(chks, poss, Tokenizer.computeOffsets(strs, strs.mkString(" ")))
      Timing.timeThen {
        val relnounExtrs = {
          try {
            val extrs = relnoun(tokens map MorphaStemmer.lemmatizePostaggedToken)
            extrs.map { inst =>
              new TripleExtraction(0.8, corpus, inst.sent.toIndexedSeq, inst.extr.arg1.text, inst.extr.rel.text, inst.extr.arg2.text, inst.extr.arg1.tokenInterval, inst.extr.rel.tokenInterval, inst.extr.arg2.tokenInterval, url)
            }
          }
          catch {
            case e: Exception =>
              System.err.println("Exception with relnouns on: " + sentence)
              System.err.println("Exception with relnouns on: " + tokens)
              e.printStackTrace()
              Nil
          }
        }

        val (srlieNaryExtrs, srlieTripleExtrs) = {
          try {
            val extrs = srlie(dgraph).filter(_.extr.arg2s.size > 0)
            val naryExtrs = extrs.map { inst =>
              val conf = srlieConf(inst)
              new NaryExtraction(conf, corpus, tokens.toIndexedSeq,
                  NaryExtraction.Part(inst.extr.arg1.text, inst.extr.arg1.interval),
                  NaryExtraction.Part(inst.extr.rel.text, inst.extr.rel.span),
                  inst.extr.arg2s.map(arg2 => NaryExtraction.Part(arg2.text, arg2.interval)).toList,
                  url)
            }
            val tripleExtrs = extrs.flatMap(_.triplize()).map { inst =>
              val conf = srlieConf(inst)
              new TripleExtraction(conf, corpus, tokens.toIndexedSeq, inst.extr.arg1.text, inst.extr.rel.text, inst.extr.arg2s.head.text, inst.extr.arg1.interval, inst.extr.rel.span, inst.extr.arg2s.head.interval, url)
            }

            (naryExtrs, tripleExtrs)
          }
          catch {
            case e: Exception =>
              System.err.println("Exception with relnouns on: " + sentence)
              System.err.println("Exception with relnouns on: " + dgraph.serialize)
              e.printStackTrace()
              (Nil, Nil)
          }
        }

        val triples = relnounExtrs ++ srlieTripleExtrs
        val nary = srlieNaryExtrs ++ relnounExtrs.map { extr =>
          new NaryExtraction(extr.confidence, extr.corpus, extr.sentenceTokens, NaryExtraction.Part(extr.arg1Text, extr.arg1Interval), NaryExtraction.Part(extr.relText, extr.relInterval), List(NaryExtraction.Part(extr.arg2Text, extr.arg2Interval)), extr.sourceUrl)
        }

        (triples, nary)
      } { ns =>
        if (ns > 60 * Timing.Seconds.divisor) {
          System.err.println(s"Long time ${Timing.Seconds.format(ns)} to process: " + Timing.Seconds.format(ns))
        }
      }
    }

    def split(str: String) = wsSplit.split(str)

    val finalExtractions = lines.mapFlatten { line =>
      tabSplit.split(line) match {
        case Array(_, url, _, _, sentence, strs, poss, chks, dependencies, _*) => {
          val (tripleExtrs, naryExtrs) = getExtractions(sentence, split(strs), split(poss), split(chks), DependencyGraph.deserialize(dependencies), url)
          (tripleExtrs map TripleExtraction.serializeToString map ("T\t" + _)) ++
            (naryExtrs map NaryExtraction.serializeToString map ("N\t" + _))
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
