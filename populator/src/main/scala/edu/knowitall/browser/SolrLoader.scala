package edu.washington.cs.knowitall.browser.solr

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.{MalformedURLException, URL}

import scala.actors.Futures.future
import scala.actors.threadpool.AtomicInteger
import scala.collection.JavaConverters.{asJavaIteratorConverter, seqAsJavaListConverter, setAsJavaSetConverter}
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
import net.liftweb.json.{compact, render}
import net.liftweb.json.JObject
import net.liftweb.json.JsonAST.JDouble
import net.liftweb.json.JsonDSL.{int2jvalue, jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue}
import scopt.immutable.OptionParser
import sun.misc.BASE64Encoder

abstract class SolrLoader {
  type REG = ExtractionGroup[ReVerbExtraction]

  def close()
  def post(reg: REG)
  def post(regs: Iterator[REG])
}

class SolrJLoader(urlString: String) extends SolrLoader {
  val solr = new HttpSolrServer(urlString)
  val id = new AtomicInteger(0)

  def close() {}

  def toSolrDocument(reg: REG) = {
    val document = new SolrInputDocument();

    lazy val instanceBytes = using(new ByteArrayOutputStream()) { bos =>
      using(new ObjectOutputStream(bos)) { out =>
        out.writeObject(reg.instances.toList)
      }
      bos.toByteArray()
    }

    for (f <- reg.getClass.getDeclaredFields) {
      document.setField("id", id.getAndIncrement())
      document.setField("arg1", reg.arg1.norm)
      document.setField("rel", reg.rel.norm)
      document.setField("arg2", reg.arg2.norm)

      reg.arg1.entity.map { entity =>
        document.setField("arg1_entity_id", entity.fbid)
        document.setField("arg1_entity_name", entity.name)
        document.setField("arg1_entity_inlink_ratio", entity.inlinkRatio)
        document.setField("arg1_entity_score", entity.score)
      }
      document.setField("arg1_fulltypes", reg.arg1.types.map(_.name).asJava)
      document.setField("arg1_types", reg.arg1.types.map(_.typ).asJava)

      reg.arg2.entity.map { entity =>
        document.setField("arg2_entity_id", entity.fbid)
        document.setField("arg2_entity_name", entity.name)
        document.setField("arg2_entity_inlink_ratio", entity.inlinkRatio)
        document.setField("arg2_entity_score", entity.score)
      }
      document.setField("arg2_fulltypes", reg.arg2.types.map(_.name).asJava)
      document.setField("arg2_types", reg.arg2.types.map(_.typ).asJava)
      
      reg.rel.link.map { link =>
        document.setField("rel_link_id", link)
      }

      document.setField("corpora", reg.instances.map(_.corpus).toList.asJava)
      document.setField("instances", instanceBytes)
      document.setField("size", reg.instances.size)
    }

    document
  }

  def post(reg: REG) {
    solr.add(toSolrDocument(reg))
  }

  def post(regs: Iterator[REG]) = {
    solr.add((regs map toSolrDocument).asJava)
  }
}

class JsonSolrLoader(solrUrl: String) extends SolrLoader {
  import dispatch._

  val b64 = new BASE64Encoder()
  val http = new Http()

  val svc = url(solrUrl)

  def close() {
    http.shutdown()
  }

  def toJsonObject(reg: REG): JObject = {
    lazy val instanceBytes = using(new ByteArrayOutputStream()) { bos =>
      using(new ObjectOutputStream(bos)) { out =>
        out.writeObject(reg.instances.toSeq.toStream)
      }
      bos.toByteArray()
    }

    val fieldMap =
      ("arg1" -> reg.arg1.norm) ~
        ("rel" -> reg.rel.norm) ~
        ("arg2" -> reg.arg2.norm) ~
        ("arg1_entity_id" -> reg.arg1.entity.map(_.fbid)) ~
        ("arg1_entity_name" -> reg.arg1.entity.map(_.name)) ~
        ("arg1_entity_inlink_ratio" -> reg.arg1.entity.map(x => JDouble(x.inlinkRatio))) ~
        ("arg1_entity_score" -> reg.arg1.entity.map(x => JDouble(x.score))) ~
        ("arg1_fulltypes" -> reg.arg1.types.map(_.name)) ~
        ("arg1_types" -> reg.arg1.types.map(_.typ)) ~
        ("arg2_entity_id" -> reg.arg2.entity.map(_.fbid)) ~
        ("arg2_entity_name" -> reg.arg2.entity.map(_.name)) ~
        ("arg2_entity_inlink_ratio" -> reg.arg2.entity.map(x => JDouble(x.inlinkRatio))) ~
        ("arg2_entity_score" -> reg.arg2.entity.map(x => JDouble(x.score))) ~
        ("arg2_fulltypes" -> reg.arg2.types.map(_.name)) ~
        ("arg2_types" -> reg.arg2.types.map(_.typ)) ~
        ("rel_link_id" -> reg.rel.link) ~
        ("corpora" -> reg.instances.map(_.corpus).toList) ~
        ("instances" -> b64.encode(instanceBytes).replaceAll("\n", "")) ~
        ("size" -> reg.instances.size)

    fieldMap
  }

  def toJsonString(reg: REG): String = {
    val json = toJsonObject(reg)
    compact(render(json))
  }

  def post(regs: Iterator[REG]): Unit = {
    val updateSvc = svc / "update"
    val postData = (regs map toJsonString).mkString("[", ",", "]")
    val headers = Map("Content-type" -> "application/json")
    val req = updateSvc <:< headers << postData
    http(req OK as.String).apply()
  }

  def post(reg: REG): Unit = {
    val updateSvc = svc / "update"
    val postData = toJsonString(reg)
    val headers = Map("Content-type" -> "application/json")
    val req = updateSvc <:< headers << postData
    http(req OK as.String).apply()
  }
}

object SolrLoader {
  import edu.knowitall.browser.lucene.ParallelIndexPrinter

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
      Source.fromURL(url).getLines map ReVerbExtractionGroup.deserializeFromString map (_.get)
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
        arg("source", "source") { (str: String, c: Config) =>
          str match {
            case "stdin" => c.copy(source = StdinSource())
            case "lucene" => c.copy(source = LuceneSource())
            case url if control.Exception.catching(classOf[MalformedURLException]) opt new URL(url) isDefined => c.copy(source = UrlSource(url))
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
    SolrLoader.logger.info("Importing from " + config.source + " to " + config.url)
    using(new SolrJLoader(config.url)) { loader =>
      val regs = config.source.groupIterator

      if (config.stdOut) regs map loader.asInstanceOf[JsonSolrLoader].toJsonString foreach println
      else {
        val lock = new Object()
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
              case e => e.printStackTrace
            }
          }
        } else {
          import scala.actors.Futures._
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
              case e => e.printStackTrace
            }
          }}) foreach (_.apply())
        }
      }
    }
  }
}