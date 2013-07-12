package edu.knowitall.browser.solr

import com.twitter.bijection.Bijection
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.{MalformedURLException, URL}
import scala.concurrent._
import scala.collection.JavaConverters.{asJavaIteratorConverter, seqAsJavaListConverter, setAsJavaSetConverter}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.control
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.common.SolrInputDocument
import org.slf4j.LoggerFactory
import dispatch.{Http, as, enrichFuture, implyRequestHandlerTuple, implyRequestVerbs, url}
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
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.openie.models.Extraction
import edu.knowitall.openie.models.serialize.TabReader

abstract class SolrLoader {
  def close()
  def post(cluster: ExtractionCluster[Extraction])
  def post(clusters: Iterator[ExtractionCluster[Extraction]])
}

class SolrJLoader(urlString: String) extends SolrLoader {
  val solr = new HttpSolrServer(urlString)
  val id = new AtomicInteger(0)
  val kryos = new ThreadLocal[Bijection[AnyRef, Array[Byte]]] {
    override def initialValue() = {
      Chill.createBijection()
    }
  }

  def close() {}

  def toSolrDocument(cluster: ExtractionCluster[Extraction]) = {
    val document = new SolrInputDocument();

    val kryo = kryos.get() // thread local instance
    val instanceBytes = kryo(cluster.instances.toList)

    for (f <- cluster.getClass.getDeclaredFields) {
      document.setField("id", id.getAndIncrement())
      document.setField("arg1", cluster.arg1.norm)
      document.setField("rel", cluster.rel.norm)
      document.setField("arg2", cluster.arg2.norm)

      cluster.arg1.entity.map { entity =>
        document.setField("arg1_entity_id", entity.fbid)
        document.setField("arg1_entity_name", entity.name)
        document.setField("arg1_entity_inlink_ratio", entity.inlinkRatio)
        document.setField("arg1_entity_score", entity.score)
      }
      document.setField("arg1_fulltypes", cluster.arg1.types.map(_.name).asJava)
      document.setField("arg1_types", cluster.arg1.types.map(_.typ).asJava)

      cluster.arg2.entity.map { entity =>
        document.setField("arg2_entity_id", entity.fbid)
        document.setField("arg2_entity_name", entity.name)
        document.setField("arg2_entity_inlink_ratio", entity.inlinkRatio)
        document.setField("arg2_entity_score", entity.score)
      }
      document.setField("arg2_fulltypes", cluster.arg2.types.map(_.name).asJava)
      document.setField("arg2_types", cluster.arg2.types.map(_.typ).asJava)

      document.setField("corpora", cluster.instances.map(_.corpus).toList.asJava)
      document.setField("instances", instanceBytes)
      document.setField("size", cluster.instances.size)
    }

    document
  }

  def post(cluster: ExtractionCluster[Extraction]) {
    solr.add(toSolrDocument(cluster))
  }

  def post(clusters: Iterator[ExtractionCluster[Extraction]]) = {
    solr.add((clusters map toSolrDocument).asJava)
  }
}

class SolrJsonLoader(solrUrl: String) extends SolrLoader {
  import dispatch._

  val kryo = Chill.createBijection()

  val b64 = new BASE64Encoder()
  val http = new Http()

  val svc = url(solrUrl)

  def close() {
    http.shutdown()
  }

  def toJsonObject(cluster: ExtractionCluster[Extraction]): JObject = {
    val instanceBytes = kryo(cluster.instances.toList)
    /*
    lazy val instanceBytes = using(new ByteArrayOutputStream()) { bos =>
      using(new ObjectOutputStream(bos)) { out =>
        out.writeObject(cluster.instances.toSeq.toStream)
      }
      bos.toByteArray()
    }
    */

    val fieldMap =
      ("arg1" -> cluster.arg1.norm) ~
        ("rel" -> cluster.rel.norm) ~
        ("arg2" -> cluster.arg2.norm) ~
        ("arg1_entity_id" -> cluster.arg1.entity.map(_.fbid)) ~
        ("arg1_entity_name" -> cluster.arg1.entity.map(_.name)) ~
        ("arg1_entity_inlink_ratio" -> cluster.arg1.entity.map(x => JDouble(x.inlinkRatio))) ~
        ("arg1_entity_score" -> cluster.arg1.entity.map(x => JDouble(x.score))) ~
        ("arg1_fulltypes" -> cluster.arg1.types.map(_.name)) ~
        ("arg1_types" -> cluster.arg1.types.map(_.typ)) ~
        ("arg2_entity_id" -> cluster.arg2.entity.map(_.fbid)) ~
        ("arg2_entity_name" -> cluster.arg2.entity.map(_.name)) ~
        ("arg2_entity_inlink_ratio" -> cluster.arg2.entity.map(x => JDouble(x.inlinkRatio))) ~
        ("arg2_entity_score" -> cluster.arg2.entity.map(x => JDouble(x.score))) ~
        ("arg2_fulltypes" -> cluster.arg2.types.map(_.name)) ~
        ("arg2_types" -> cluster.arg2.types.map(_.typ)) ~
        ("corpora" -> cluster.instances.map(_.corpus).toList) ~
        ("instances" -> b64.encode(instanceBytes).replaceAll("\n", "")) ~
        ("size" -> cluster.instances.size)

    fieldMap
  }

  def toJsonString(cluster: ExtractionCluster[Extraction]): String = {
    val json = toJsonObject(cluster)
    compact(render(json))
  }

  def post(clusters: Iterator[ExtractionCluster[Extraction]]): Unit = {
    val updateSvc = svc / "update"
    val postData = (clusters map toJsonString).mkString("[", ",", "]")
    val headers = Map("Content-type" -> "application/json")
    val req = updateSvc <:< headers << postData
    http(req OK as.String).apply()
  }

  def post(cluster: ExtractionCluster[Extraction]): Unit = {
    val updateSvc = svc / "update"
    val postData = toJsonString(cluster)
    val headers = Map("Content-type" -> "application/json")
    val req = updateSvc <:< headers << postData
    http(req OK as.String).apply()
  }
}

object SolrLoader {

  val logger = LoggerFactory.getLogger(classOf[SolrLoader])

  sealed abstract class Source {
    def groupIterator(): Iterator[ExtractionCluster[Extraction]]
    def close(): Unit
  }

  case class StdinSource() extends Source {
    def groupIterator() = {
      Source.stdin.getLines map implicitly[TabReader[ExtractionCluster[Extraction]]].read map (_.get)
    }

    def close() {}
  }

  case class UrlSource(url: String) extends Source {
    def groupIterator() = {
      Source.fromURL(url, "UTF-8").getLines map implicitly[TabReader[ExtractionCluster[Extraction]]].read map (_.get)
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
      val clusters = config.source.groupIterator

      if (config.stdOut) clusters map loader.asInstanceOf[SolrJsonLoader].toJsonString foreach println
      else {
        val lock = new Object()
        val index = new AtomicInteger(0)
        val start = System.nanoTime
        val BATCH_SIZE = 1000
        if (!config.parallel) {
          clusters.grouped(BATCH_SIZE).foreach { cluster =>
            try {
              Timing.timeThen {
                loader.post(cluster.iterator)
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
          clusters.grouped(BATCH_SIZE).map( { cluster => future {
            try {
              Timing.timeThen {
                loader.post(cluster.iterator)
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
}
