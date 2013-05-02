package edu.knowitall.browser.lucene

import java.io.File
import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.util.matching.Regex
import scala.io.Source
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.search.TopFieldCollector
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.util.Version
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.IndexReader

import scopt.OptionParser

import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.InstanceDeduplicator
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.tool.stem.MorphaStemmer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Stopwatch

class ParallelExtractionGroupFetcher(val simpleFetchers: Seq[ExtractionGroupFetcher]) extends GroupFetcher {

  require(!simpleFetchers.isEmpty)

  val logger = LoggerFactory.getLogger(this.getClass)

  def this(indexPaths: Seq[String], searchMaxGroups: Int, readMaxInstances: Int, timeoutMillis: Long, stoplist: Boolean = true) =
    this(indexPaths.map(path => new ExtractionGroupFetcher(path, searchMaxGroups/indexPaths.size, readMaxInstances/indexPaths.size, timeoutMillis, stoplist)))

  def getGroups(querySpec: QuerySpec): ResultSet = {
    val parallelwatch = new Stopwatch().start()
    logger.info("Parallel lucene query: %s".format(querySpec.luceneQueryString))
    val allResults = simpleFetchers.map(fetcher => future { fetcher.getGroups(querySpec) }).map(Await.result(_, 60 seconds))
    val combinedResults = allResults.reduceOption(_.combineWith(_)).getOrElse(Success.empty)

    logger.info("Parallel %s yields %s groups, %s instances in %s ms"
        .format(
            querySpec.toString,
            combinedResults.numGroups,
            combinedResults.numInstances,
            parallelwatch.elapsed(MILLISECONDS)))

    combinedResults
  }

  def close() = simpleFetchers.foreach(_.close())
}

object ParallelExtractionGroupFetcher {

  private val whiteSpaceSplitter = "\\s+".r

  val defaultIndexes = "/scratch/common/openie-demo/index-1.0.4:/scratch2/common/openie-demo/index-1.0.4:/scratch3/common/openie-demo/index-1.0.4:/scratch4/common/openie-demo/index-1.0.4"

  def main(args: Array[String]): Unit = {

    var indexPaths = defaultIndexes.split(":")
    var searchMaxGroups = 1000
    var readMaxInstances = 1000
    var deduplicate = false
    var frontendGroup = false
    var timeoutMillis = 10000L
    var stem = true
    var prettyPrint = true

    var arg1: Option[String]       = None
    var rel: Option[String]        = None
    var arg2: Option[String]       = None
    var arg1Entity: Option[String] = None
    var arg2Entity: Option[String] = None
    var arg1Fbid: Option[String]   = None
    var arg2Fbid: Option[String]   = None
    var arg1Types: Option[String]  = None
    var arg2Types: Option[String]  = None
    var corpora: Option[String]    = None
    var noInst = false
    var entityStoplist = false

    val optionParser = new OptionParser() {
      opt("paths", "paths to browser indexes to query, default=%s".format(defaultIndexes), { str => indexPaths = str.split(":") })

      opt("arg1", "Query for argument 1 string", { str => arg1 = Some(str) } )
      opt("rel", "Query for relation string", { str => rel = Some(str) } )
      opt("arg2", "Query for argument 2 string", { str => arg2 = Some(str) } )
      opt("arg1Entity", "Query for arg1 entity name (case sensitive)", { str => arg1Entity = Some(str) } )
      opt("arg2Entity", "Query for arg2 entity name (case sensitive)", { str => arg2Entity = Some(str) } )
      opt("arg1Fbid", "Query for arg1 freebase id", { str => arg1Fbid = Some(str) } )
      opt("arg2Fbid", "Query for arg2 freebase id", { str => arg2Fbid = Some(str) } )
      opt("arg1Types", "Query for arg1 type name (e.g. book_subject)", { str => arg1Types = Some(str) } )
      opt("arg2Types", "Query for arg2 type name", { str => arg2Types = Some(str) } )
      opt("corpora", "Query for particular source corpora, e.g. cw, g1b, news, nell", { str => corpora = Some(str) })
      opt("timeout", "query timeout milliseconds", { str => timeoutMillis = str.toLong })
      opt("maxGroups", "Max docs to search for", { str => searchMaxGroups = str.toInt })
      opt("maxInstances", "Max instances to read in total", { str => readMaxInstances = str.toInt })
      opt("noInst", "Do not list instances, only show Tuple line", { noInst = true })
      opt("entityStoplist", "Use an entity stoplist to reduce systematic linker errors", { entityStoplist = true })
      opt("d", "deduplicate instances", { deduplicate = true })
      opt("g", "re-group to frontend grouping", { frontendGroup = true })
      opt("ns", "do not stem query", { stem = false })

      opt("tabOutput", "Use tab-delimited ReVerbExtractionGroup format (less human readable)", { prettyPrint = false })
    }

    def dedupOpt(group: ExtractionGroup[ReVerbExtraction]) = if (deduplicate) InstanceDeduplicator.deduplicate(group) else group

    if (!optionParser.parse(args)) return

    if (Seq(arg1, rel, arg2, arg1Entity, arg2Entity, arg1Types, arg2Types, arg1Fbid, arg2Fbid).flatten.length == 0) {
      System.err.println("You must query on at least one of arg1, rel, arg2, arg1Entity, arg2Entity, arg1Types, or arg2Types")
      return
    }

    val parFetcher = new ParallelExtractionGroupFetcher(indexPaths, searchMaxGroups, readMaxInstances, timeoutMillis, stoplist = entityStoplist)

    val querySpec = QuerySpec(arg1, rel, arg2, arg1Entity, arg2Entity, arg1Types, arg2Types, corpora, stem, true, arg1Fbid, arg2Fbid)
    val results = parFetcher.getGroups(querySpec)

    def tabPrintResults(groups: Seq[ExtractionGroup[ReVerbExtraction]], resultType: String): Unit = {
      groups.foreach{ group =>
        println(ReVerbExtractionGroup.serializeToString(group))
      }
    }

    def prettyPrintResults(groups: Seq[ExtractionGroup[ReVerbExtraction]], resultType: String): Unit = {

      println()
      println()
      println("Number of unique tuples: %d, Total instances in all tuples: %d, Query Status: %s".format(groups.size, groups.map(_.instances.size).sum, resultType))
      println()

      groups.foreach { group =>
        val basicHeaderString = "Group(%d): (%s, %s, %s)".format(group.instances.size, group.arg1.norm, group.rel.norm, group.arg2.norm)
        val arg1EntityString = group.arg1.entity match {
          case Some(entity) => "Link(Name:%s, ID:%s)".format(entity.name, entity.fbid)
          case None => "No link"
        }
        val arg1TypesString = if (group.arg1.types.isEmpty) "No types" else group.arg1.types.map(_.name).mkString(",")

        val arg2EntityString = group.arg2.entity match {
          case Some(entity) => "Link(Name:%s, ID:%s)".format(entity.name, entity.fbid)
          case None => "No link"
        }
        val arg2TypesString = if (group.arg2.types.isEmpty) "No types" else group.arg2.types.map(_.name).mkString(",")

        val printString =
          "%s (Arg1: %s %s, Arg2: %s %s)".format(basicHeaderString, arg1EntityString, arg1TypesString, arg2EntityString, arg2TypesString)
        println(printString)
        if (!noInst) {
          group.instances.toSeq.sortBy(-_.confidence).foreach { inst =>
            val str =
              "         %s\t%.03f\t%s\t%s".format(inst.extraction.sentenceTokens.map(_.string).mkString(" "), inst.confidence, inst.corpus, inst.extraction.sourceUrl)
            println(str)
          }
          println()
        }
      }
    }

    def processResults(results: Seq[ExtractionGroup[ReVerbExtraction]], resultType: String): Unit = {
      val processedResults = if (frontendGroup) ReVerbExtractionGroup.indexGroupingToFrontendGrouping(results).toSeq else results
      val finalResults = processedResults.map(dedupOpt(_)).sortBy(-_.instances.size)
      if (prettyPrint) prettyPrintResults(finalResults, resultType)
      else tabPrintResults(finalResults, resultType)
    }

    results match {
      case Success(results) => processResults(results, "Success")
      case Timeout(results, _) => processResults(results, "Timeout")
      case Limited(results, _) => processResults(results, "Limited")
    }
  }
}
