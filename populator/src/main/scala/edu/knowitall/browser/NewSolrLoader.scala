package edu.knowitall.browser.solr

import java.io.{ByteArrayOutputStream, ObjectOutputStream, File}
import java.net.{MalformedURLException, URL}
import scala.concurrent._
import scala.collection.JavaConverters.{asJavaIteratorConverter, seqAsJavaListConverter, setAsJavaSetConverter}
import scala.collection.mutable.MutableList
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.control
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.common.SolrInputDocument
import org.slf4j.LoggerFactory
import dispatch.{Http, as, enrichFuture, implyRequestHandlerTuple, implyRequestVerbs, url}
import edu.knowitall.browser.lucene.ParallelIndexPrinter
import edu.knowitall.common.Resource.using
import edu.knowitall.common.Timing
import edu.knowitall.openie.models.{ExtractionGroup, ReVerbExtraction, ReVerbExtractionGroup}
import edu.knowitall.openie.models.serialize.Chill
import net.liftweb.json.{compact, render}
import net.liftweb.json.JObject
import net.liftweb.json.JsonAST.JDouble
import net.liftweb.json.JsonDSL.{int2jvalue, jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue}
import scopt.immutable.OptionParser
import sun.misc.BASE64Encoder
import java.util.concurrent.atomic.AtomicInteger

abstract class NewSolrLoader {
  def close()
  def post(group: ExtractionGroup[ReVerbExtraction])
  def post(groups: Iterator[ExtractionGroup[ReVerbExtraction]])
}

class NewSolrJLoader(urlString: String) extends NewSolrLoader {

  type REG = ExtractionGroup[ReVerbExtraction]
  
  val solr = new HttpSolrServer(urlString)
  val id = new AtomicInteger(0)

  def close() {}

  def toSolrDocuments(reg: REG) = {

    val relation = new SolrInputDocument()
    var docs = Seq(relation)

    relation.setField("id", id.getAndIncrement())
    relation.setField("doctype", "relation")
    relation.setField("arg1", reg.arg1.norm)
    relation.setField("rel", reg.rel.norm)
    relation.setField("arg2", reg.arg2.norm)

    reg.arg1.entity.map { entity =>
      relation.setField("arg1_entity_id", entity.fbid)
      relation.setField("arg1_entity_name", entity.name)
      relation.setField("arg1_entity_inlink_ratio", entity.inlinkRatio)
      relation.setField("arg1_entity_score", entity.score)
    }
    relation.setField("arg1_fulltypes", reg.arg1.types.map(_.name).asJava)
    relation.setField("arg1_types", reg.arg1.types.map(_.typ).asJava)

    reg.arg2.entity.map { entity =>
      relation.setField("arg2_entity_id", entity.fbid)
      relation.setField("arg2_entity_name", entity.name)
      relation.setField("arg2_entity_inlink_ratio", entity.inlinkRatio)
      relation.setField("arg2_entity_score", entity.score)
    }
    relation.setField("arg2_fulltypes", reg.arg2.types.map(_.name).asJava)
    relation.setField("arg2_types", reg.arg2.types.map(_.typ).asJava)

    relation.setField("corpora", reg.instances.map(_.corpus).toList.distinct.asJava)
    relation.setField("size", reg.instances.size)

    for(instance <- reg.instances) {

        val extraction = new SolrInputDocument()

        val regId = id.getAndIncrement(); 
        extraction.setField("id", regId);
        extraction.setField("doctype", "reverb_extraction")

        extraction.setField("sentence_text", instance.extraction.sentenceText)
        
        val tripleNorm = instance.extraction.indexGroupingKey
        extraction.setField("arg1", tripleNorm._1)
        extraction.setField("rel", tripleNorm._2)
        extraction.setField("arg2", tripleNorm._3)

        extraction.setField("corpus", instance.corpus)

        val offsets = new MutableList[Int]()
        val postags = new MutableList[String]()
        val chunks = new MutableList[String]()

        for(tok <- instance.extraction.sentenceTokens) {

            offsets += tok.offset
            postags += tok.postag
            chunks += tok.chunk
        }

        extraction.setField("arg1_interval", instance.extraction.arg1Interval.toString)
        extraction.setField("rel_interval", instance.extraction.relInterval.toString)
        extraction.setField("arg2_interval", instance.extraction.arg2Interval.toString)
        extraction.setField("url", instance.extraction.sourceUrl)
        extraction.setField("term_offsets", offsets.mkString(","))
        extraction.setField("postags", postags.mkString(","))
        extraction.setField("chunks", chunks.mkString(","))

        relation.addField("source_ids", regId)

        docs = extraction +: docs
    }
    
    docs

  }

  def post(reg: REG) {
    val resp = solr.add(toSolrDocuments(reg).asJava)
    solr.commit()
  }

  def post(regs: Iterator[REG]) = {
    val resp = solr.add((regs map toSolrDocuments).flatten.asJava)
    solr.commit()
  }
}

object NewSolrLoader {

  val logger = LoggerFactory.getLogger(classOf[SolrLoader])

  sealed abstract class Source {
    def groupIterator(): Iterator[ExtractionGroup[ReVerbExtraction]]
    def close(): Unit
  }

  case class LuceneSource() extends Source {
    val printer = ParallelIndexPrinter.defaultInstance

    def groupIterator() = {
      printer.getRegs
    }

    def close() {}
  }

  case class StdinSource() extends Source {
    def groupIterator() = {
      Source.stdin.getLines map ReVerbExtractionGroup.deserializeFromString map (_.get)
    }

    def close() {}
  }

  case class UrlSource(url: String) extends Source {
    def groupIterator() = {
      Source.fromURL(url, "UTF-8").getLines map ReVerbExtractionGroup.deserializeFromString map (_.get)
    }

    def close() {}
  }

  case class DirectorySource(file: File) extends Source {
    require(file.exists, "file does not exist: " + file)

    val files = 
      if (file.isDirectory) file.listFiles.toList
      else List(file)

    files.foreach(file => logger.info("Appending file to import: " + file))

    def groupIterator() = {
      val thunks: Iterator[() => Iterator[ExtractionGroup[ReVerbExtraction]]] = files.iterator.map(file => 
        () => 
          Source.fromFile(file, "UTF-8").getLines map ReVerbExtractionGroup.deserializeFromString map (_.get)
      )

      // i'm ignoring closing handles--resource leak
      thunks.foldLeft(thunks.next.apply()){ case (it, th) => it ++ th.apply() }
    }

    def close() {}
  }

  case class Config(
    val source: Source = StdinSource(),
    val url: String = "",
    val stdOut: Boolean = false,
    val count: Int = Int.MaxValue,
    val parallel: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config]("SolrLoader") {
      def options = Seq(
        arg("solr-url", "solr url") { (str: String, c: Config) => c.copy(url = str) },
        arg("source", "source (stdin|lucene|url)") { (str: String, c: Config) =>
          str match {
            case "stdin" => c.copy(source = StdinSource())
            case "lucene" => c.copy(source = LuceneSource())
            case path if new File(path).exists => c.copy(source = DirectorySource(new File(path))) 
            case url if control.Exception.catching(classOf[MalformedURLException]) opt new URL(url) isDefined => c.copy(source = UrlSource(url))
            case source => throw new IllegalArgumentException("Unknown source (not a file or URL): " + source)
          }
        },
        flag("stdout", "dump to stdout") { (c: Config) => c.copy(stdOut = true) },
        flag("p", "parallel", "import in parallel") { (c: Config) => c.copy(parallel = true) },
        intOpt("n", "number to process") { (n: Int, c: Config) => c.copy(count = n) })
    }

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None =>
    }
  }

  def run(config: Config) = {

    NewSolrLoader.logger.info("Importing from " + config.source + " to " + config.url)

    using(new NewSolrJLoader(config.url)) { loader =>

      val regs = config.source.groupIterator

      val index = new AtomicInteger(0)
      val start = System.nanoTime
      val BATCH_SIZE = 1000
      if (!config.parallel) {
        regs.grouped(BATCH_SIZE).foreach { reg =>
          try {
            Timing.timeThen {
            loader.post(reg.iterator)
            } { ns =>
              val i = index.getAndIncrement()
              val elapsed = System.nanoTime - start
              println("Batch " + i + " (" + i * BATCH_SIZE + ") in " + Timing.Seconds.format(ns) + " total " + Timing.Seconds.format(elapsed) + " avg " + Timing.Seconds.format(elapsed / index.get) + ".")
            }
          } catch {
            case e: Throwable => e.printStackTrace
          }
        }
      } else {
        regs.grouped(BATCH_SIZE).map( { reg => future {
          try {
            Timing.timeThen {
              loader.post(reg.iterator)
            } { ns =>
              val i = index.getAndIncrement()
              val elapsed = System.nanoTime - start
              println("Batch " + i + " (" + i * BATCH_SIZE + ") in " + Timing.Seconds.format(ns) + " total " + Timing.Seconds.format(elapsed) + " avg " + Timing.Seconds.format(elapsed / index.get) + ".")
            }
          } catch {
            case e: Throwable => e.printStackTrace
          }
        }}) foreach (_.apply())
      }
    }
  }
}
