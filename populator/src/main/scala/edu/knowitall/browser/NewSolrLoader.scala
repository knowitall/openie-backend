package edu.knowitall.browser.solr

import java.io.{ByteArrayOutputStream, ObjectOutputStream, File}
import java.net.{MalformedURLException, URL}
import scala.concurrent._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import scala.collection.mutable.MutableList
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.control
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer
import org.apache.solr.common.SolrInputDocument
import org.slf4j.LoggerFactory
import dispatch.{Http, as, enrichFuture, implyRequestHandlerTuple, implyRequestVerbs, url}
import edu.knowitall.common.Resource.using
import edu.knowitall.common.Timing
import edu.knowitall.openie.models.{Extraction, ExtractionCluster, ExtractionGroup, ReVerbExtraction, ReVerbExtractionGroup}
import edu.knowitall.openie.models.serialize.Chill
import net.liftweb.json.{compact, render}
import net.liftweb.json.JObject
import net.liftweb.json.JsonAST.JDouble
import net.liftweb.json.JsonDSL.{int2jvalue, jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue}
import scopt.immutable.OptionParser
import sun.misc.BASE64Encoder
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.io.FileUtils
import edu.knowitall.openie.models.serialize.TabReader

class NewSolrJLoader(urlString: String) extends SolrLoader {

  import SolrDocumentConverter.toSolrDocuments

  type REG = ExtractionGroup[ReVerbExtraction]

  val solr = new ConcurrentUpdateSolrServer(urlString, 10000, 4)
  val id = new AtomicLong(0)

  def idFactory() = id.getAndIncrement().toString

  def close() { solr.commit(); }

  def post(cluster: ExtractionCluster[Extraction]) {
    val resp = solr.add(toSolrDocuments(cluster, idFactory).asJava)
    //solr.commit()
  }

  def post(clusters: Iterator[ExtractionCluster[Extraction]]) = {

    val resp = solr.add(clusters.map(c => toSolrDocuments(c, idFactory)).flatten.toSeq.asJava)
    //solr.commit()
  }
}

object SolrDocumentConverter {

  def toSolrDocuments(cluster: ExtractionCluster[Extraction], idFactory: () => String) = {

    def fail(s: String) = {
      System.err.println(s)
      Nil
    }

    try {
      unsafeToSolrDocuments(cluster, idFactory)
    } catch {
      case e: java.lang.AssertionError => fail(e.getMessage())
      case e: Exception => fail(e.getMessage())
    }
  }

  // wrap toSolrDocuments in try-catch...
  private def unsafeToSolrDocuments(cluster: ExtractionCluster[Extraction], idFactory: () => String) = {

    val relation = new SolrInputDocument()
    var docs = Seq(relation)
    // Choose the most common un-normalized triple...
    // This is necessary so that the _exact fields aren't normalized.
    // Ideally the cluster should have a non-normalized triple
    val (arg1, rel, arg2) = {
      val triples = cluster.instances.map(extr => (extr.arg1Text, extr.relText, extr.arg2Text))
      val tripleCounts = triples.groupBy(identity).iterator.map(kv => (kv._1, kv._2.size))
      tripleCounts.toSeq.sortBy(-_._2).head._1
    }

    relation.setField("id", idFactory())
    relation.setField("doctype", "openie4")
    relation.setField("arg1", arg1)
    relation.setField("rel", rel)
    relation.setField("arg2", arg2)

    cluster.arg1.entity.map { entity =>
      relation.setField("arg1_entity_id", entity.fbid)
      relation.setField("arg1_entity_name", entity.name)
      relation.setField("arg1_entity_inlink_ratio", entity.inlinkRatio)
      relation.setField("arg1_entity_score", entity.score)
    }
    relation.setField("arg1_fulltypes", cluster.arg1.types.map(_.name).asJava)
    relation.setField("arg1_types", cluster.arg1.types.map(_.typ).asJava)

    cluster.arg2.entity.map { entity =>
      relation.setField("arg2_entity_id", entity.fbid)
      relation.setField("arg2_entity_name", entity.name)
      relation.setField("arg2_entity_inlink_ratio", entity.inlinkRatio)
      relation.setField("arg2_entity_score", entity.score)
    }
    relation.setField("arg2_fulltypes", cluster.arg2.types.map(_.name).asJava)
    relation.setField("arg2_types", cluster.arg2.types.map(_.typ).asJava)

    relation.setField("corpora", cluster.instances.map(_.corpus).toList.distinct.asJava)
    relation.setField("size", cluster.instances.size)

    for(instance <- cluster.instances) {

        val extraction = new SolrInputDocument()

        val regId = idFactory();
        extraction.setField("id", regId);
        extraction.setField("doctype", "openie4_metadata")

        extraction.setField("sentence_text", instance.sentenceText)

//        extraction.setField("arg1", instance.arg1Text)
//        extraction.setField("rel", instance.relText)
//        extraction.setField("arg2", instance.arg2Text)

        extraction.setField("corpus", instance.corpus)

        val offsets = new MutableList[Int]()
        val postags = new MutableList[String]()
        val chunks = new MutableList[String]()

        for(tok <- instance.sentenceTokens) {

            offsets += tok.offset
            postags += tok.postag
            chunks += tok.chunk
        }

        extraction.setField("arg1_interval", instance.arg1Interval.toString)
        extraction.setField("rel_interval", instance.relInterval.toString)
        extraction.setField("arg2_interval", instance.arg2Interval.toString)
        extraction.setField("url", instance.source)
        extraction.setField("confidence", instance.confidence)
        extraction.setField("term_offsets", offsets.mkString(" "))
        extraction.setField("postags", postags.mkString(" "))
        extraction.setField("chunks", chunks.mkString(" "))

        relation.addField("source_ids", regId)

        docs = extraction +: docs
    }

    docs
  }
}

object NewSolrLoader {

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

  @deprecated("0.0.0", "Use SolrDocumentConverter instead")
  def toSolrDocuments(cluster: ExtractionCluster[Extraction], idFactory: () => String) = {
    SolrDocumentConverter.toSolrDocuments(cluster, idFactory)
  }

  case class DirectorySource(file: java.io.File) extends Source {
    require(file.exists, "file does not exist: " + file)

    val files =
      if (file.isDirectory) FileUtils.listFiles(file, null, true).asScala.toList
      else List(file)

    files.foreach(file => logger.info("Appending file to import: " + file))

    def groupIterator() = {
      val thunks: Iterator[() => Iterator[ExtractionCluster[Extraction]]] = files.iterator.map(file =>
        () =>
          Source.fromFile(file, "UTF-8").getLines map implicitly[TabReader[ExtractionCluster[Extraction]]].read map (_.get)
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
        arg("source", "source (stdin|url)") { (str: String, c: Config) =>
          str match {
            case "stdin" => c.copy(source = StdinSource())
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
      val clusters = config.source.groupIterator

      if (config.stdOut) clusters map loader.asInstanceOf[SolrJsonLoader].toJsonString foreach println
      else {
        val lock = new Object()
        val index = new AtomicLong(0)
        val start = System.nanoTime
        val BATCH_SIZE = 10000
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
        loader.close()
      }
    }
  }
}
