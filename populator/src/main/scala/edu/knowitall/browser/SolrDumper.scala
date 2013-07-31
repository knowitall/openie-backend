package edu.knowitall.browser.solr

import com.twitter.bijection.Bijection
import java.io.{ByteArrayOutputStream, ObjectOutputStream, File}
import java.net.{MalformedURLException, URL}
import scala.concurrent._
import scala.collection.JavaConverters._
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
import edu.knowitall.openie.models.Extraction
import org.apache.commons.io.FileUtils
import java.io.PrintWriter
import org.apache.solr.client.solrj.SolrQuery
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.Instance

class SolrJServer(urlString: String) {
  val solr = new HttpSolrServer(urlString)
  val id = new AtomicInteger(0)
  val kryos = new ThreadLocal[Bijection[AnyRef, Array[Byte]]] {
    override def initialValue() = {
      Chill.createBijection()
    }
  }

  def close() {}

  def dump(startIndex: Int, count: Int): Iterator[ExtractionGroup[ReVerbExtraction]] = {
    val kryo = kryos.get()

    val query = new SolrQuery()
    query.setQuery("*:*")
    query.setStart(startIndex)
    query.setRows(count)
    val rsp = solr.query(query)
    for (result <- rsp.getResults().iterator().asScala) yield {
      val arg1 = result.getFieldValue("arg1").asInstanceOf[String]
      val rel = result.getFieldValue("rel").asInstanceOf[String]
      val arg2 = result.getFieldValue("arg2").asInstanceOf[String]

      val arg1EntityId = result.getFieldValue("arg1_entity_id").asInstanceOf[String]
      val arg1EntityName = result.getFieldValue("arg1_entity_name").asInstanceOf[String]
      val arg1EntityInlinkRatio = result.getFieldValue("arg1_inlink_ratio").asInstanceOf[Double]
      val arg1EntityScore = result.getFieldValue("arg1_entity_score").asInstanceOf[Double]
      val arg1Entity = arg1EntityId match {
        case null => None
        case _ => Some(FreeBaseEntity(arg1EntityName, arg1EntityId, arg1EntityScore, arg1EntityInlinkRatio))
      }

      val arg2EntityId = result.getFieldValue("arg2_entity_id").asInstanceOf[String]
      val arg2EntityName = result.getFieldValue("arg2_entity_name").asInstanceOf[String]
      val arg2EntityInlinkRatio = result.getFieldValue("arg2_inlink_ratio").asInstanceOf[Double]
      val arg2EntityScore = result.getFieldValue("arg2_entity_score").asInstanceOf[Double]
      val arg2Entity = arg2EntityId match {
        case null => None
        case _ => Some(FreeBaseEntity(arg2EntityName, arg2EntityId, arg2EntityScore, arg2EntityInlinkRatio))
      }

      val instanceBytes = result.getFieldValue("instances").asInstanceOf[Array[Byte]]

      val arg1Types = Option(result.getFieldValue("arg1_fulltypes").asInstanceOf[java.util.List[String]]).getOrElse(new java.util.ArrayList[String]()).asScala.flatMap(FreeBaseType.parse).toSet
      val arg2Types = Option(result.getFieldValue("arg2_fulltypes").asInstanceOf[java.util.List[String]]).getOrElse(new java.util.ArrayList[String]()).asScala.flatMap(FreeBaseType.parse).toSet
      val instances = kryo.inverse(instanceBytes).asInstanceOf[List[Instance[ReVerbExtraction]]]

      new ExtractionGroup[ReVerbExtraction](arg1, rel, arg2, arg1Entity, arg2Entity, arg1Types, arg2Types, instances.toSet)
    }
  }
}

object SolrDumper {
  val logger = LoggerFactory.getLogger(classOf[SolrLoader])

  case class Config(
    val dest: PrintWriter = null,
    val url: String = "",
    val stdOut: Boolean = false,
    val count: Int = Int.MaxValue,
    val blockSize: Int = 10000,
    val parallel: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config]("SolrLoader") {
      def options = Seq(
        arg("solr-url", "solr url") { (str: String, c: Config) => c.copy(url = str) },
        arg("dest", "dest (stdout|lucene|url)") { (str: String, c: Config) =>
          str match {
            case "stdout" => c.copy(dest = new PrintWriter(System.out))
            case path if !new File(path).exists => c.copy(dest = new PrintWriter(new File(path), "UTF-8"))
            case _ => throw new IllegalArgumentException("Destination already exists: " + str)
          }
        },
        flag("stdout", "dump to stdout") { (c: Config) => c.copy(stdOut = true) },
        flag("p", "parallel", "import in parallel") { (c: Config) => c.copy(parallel = true) },
        intOpt("n", "count", "number to process") { (n: Int, c: Config) => c.copy(count = n) })
    }

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None =>
    }
  }

  def run(config: Config) = {
    SolrLoader.logger.info("Dumping from " + config.url + " to " + config.dest)
    println("Starting dump...")
    using(new SolrJServer(config.url)) { solr =>
      for {
        index <- Iterator.from(0)
        startIndex = index * config.blockSize
        if startIndex < config.count
        _ = println(s"Downloading ${config.blockSize} dumps starting at: $startIndex")
        dumps = solr.dump(startIndex, config.blockSize)
      } {
        dumps map ReVerbExtractionGroup.serializeToString foreach config.dest.println
        config.dest.flush()
      }
    }
    println("done.")
  }
}
