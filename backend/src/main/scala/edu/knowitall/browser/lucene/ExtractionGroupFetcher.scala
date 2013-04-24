package edu.knowitall.browser.lucene

import java.io.File

import scala.collection.JavaConversions._

import scala.util.matching.Regex
import scala.io.Source
import scala.math._

import com.google.common.base.Stopwatch

import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.SearcherManager
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.search.TimeLimitingCollector
import org.apache.lucene.search.TopScoreDocCollector
import org.apache.lucene.util.Counter
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.IndexReader

import org.apache.lucene.search.NumericRangeQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.TopDocs
import org.apache.lucene.search.ScoreDoc

import scopt.OptionParser

import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.extraction.ReVerbExtraction
import edu.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.knowitall.browser.extraction.InstanceDeduplicator

import edu.knowitall.common.Timing
import edu.knowitall.common.Resource.using

import org.apache.lucene.search.TimeLimitingCollector.TimeExceededException

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ExtractionGroupFetcher(
  protected[lucene] val searcherManager: SearcherManager,
  val searchMaxGroups: Int,
  val readMaxInstances: Int,
  val timeoutMillis: Long,
  val fbidStoplist: Set[String]) extends GroupFetcher {
  
  private val searchGroupTolerance = searchMaxGroups / 20 // we can be low by this much
  private val readInstanceTolerance = readMaxInstances / 20

  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.info("new ExtractionGroupFetcher, maxGroups:%d, maxInstances:%d, timeout:%d".format(searchMaxGroups, readMaxInstances, timeoutMillis))
  
  def this(indexPath: String, searchMaxResults: Int, readMaxInstances: Int, timeoutMillis: Long, stoplist: Boolean = true) =
    this(ExtractionGroupFetcher.loadSearcherManager(indexPath, doWarmups=true), searchMaxResults, readMaxInstances, timeoutMillis, { if (stoplist) ExtractionGroupFetcher.entityStoplist else Set.empty[String] })

  def indexSearcher: IndexSearcher = searcherManager.acquire
    
  // A lower-level implementation of getGroups. Returns a ResultSet for query given the constraints.
  private def getGroupsHelper(query: Query, maxGroups: Int, maxInstances: Int, totalTimeoutMillis: Long): ResultSet = {

    val topDocsCollector = TopScoreDocCollector.create(maxGroups, true)
    val timeLimitCollector = new TimeLimitingCollector(topDocsCollector, TimeLimitingCollector.getGlobalCounter, totalTimeoutMillis)
    val searchwatch = new Stopwatch().start()
    try {
      indexSearcher.search(query, timeLimitCollector)
      // now materialize results
    } catch {
      case timeout: TimeExceededException => {
        // Timeout has precedence, so make sure the result is a Timeout:
        val remainingMillis = totalTimeoutMillis - searchwatch.elapsedMillis()
        return Timeout.empty.combineWith(readResultSet(topDocsCollector.topDocs(), maxInstances, remainingMillis))
      }
    }
    val remainingMillis = totalTimeoutMillis - searchwatch.elapsedMillis()
    readResultSet(topDocsCollector.topDocs(), maxInstances, remainingMillis)
  }

  private def readResultSet(topDocs: TopDocs, maxInstances: Int, readTimeout: Long): ResultSet = {
    val hitCount = topDocs.totalHits
    val groupIterator = topDocs.scoreDocs.sortBy(-_.score).iterator.map(scoreDoc => docNumToGroup(scoreDoc.doc))
    val stopwatch = new Stopwatch().start()
    var instancesCount = 0
    var timedOut = false
    var limited = false
    val resultList = groupIterator.takeWhile { group =>
      if (stopwatch.elapsedMillis() >= readTimeout) {
        timedOut = true
        false
      } else {
        var numInstances = group.instances.size
        instancesCount += numInstances
        if (instancesCount > maxInstances) {
          instancesCount -= numInstances // want to report the number returned, which doesn't include this group
          limited = true
          false
        } else true
      }
    }.toList
    if (timedOut) Timeout(resultList, hitCount)
    else if (limited) Limited(resultList, hitCount)
    else Success(resultList)
  }

  private def docNumToGroup(docNum: Int): ExtractionGroup[ReVerbExtraction] = {
    val rawGroup = ReVerbDocumentConverter.fromDocument(indexSearcher.doc(docNum))
    val filtered = filterGroup(rawGroup)
    filtered
  }

  private def filterGroup(group: ExtractionGroup[ReVerbExtraction]): ExtractionGroup[ReVerbExtraction] = {
    
    // lookup arg1 in stoplist...
    val arg1Filtered = group.arg1.entity match {
      case Some(entity) => if (fbidStoplist.contains(entity.fbid)) group.removeArg1Entity else group
      case None => group
    }
    
    val arg2Filtered = arg1Filtered.arg2.entity match {
      case Some(entity) => if (fbidStoplist.contains(entity.fbid)) arg1Filtered.removeArg2Entity else arg1Filtered
      case None => arg1Filtered
    }
    
    arg2Filtered
  }
  
  def resultLoggerString(prefix: String, resultSet: ResultSet, elapsedMillis: Long): String = resultSet match {
    case Success(_) => "%s, Success: %d groups, %d instances in %d ms".format(prefix, resultSet.numGroups, resultSet.numInstances, elapsedMillis)
    case Limited(_, _) => "%s, Limited: %d groups, %d instances in %d ms".format(prefix, resultSet.numGroups, resultSet.numInstances, elapsedMillis)
    case Timeout(_, _) => "%s, Timeout: %d groups, %d instances in %d ms".format(prefix, resultSet.numGroups, resultSet.numInstances, elapsedMillis)
  }

  def getGroups(querySpec: QuerySpec): ResultSet = {
    
    var remainingMaxGroups = searchMaxGroups
    var remainingMaxInstances = readMaxInstances
    var remainingTime = timeoutMillis
    val resultWatch = new Stopwatch().start()
    var allResults: ResultSet = Success.empty
    for (query <- querySpec.lowLevelLuceneQueries) {
      if (remainingMaxGroups > searchGroupTolerance && remainingMaxInstances > readInstanceTolerance && remainingTime > 0) {
        val resultSet = getGroupsHelper(query, remainingMaxGroups, remainingMaxInstances, remainingTime)
        remainingMaxGroups -= resultSet.numGroups
        remainingMaxInstances -= resultSet.numInstances
        remainingTime -= resultWatch.elapsedMillis
        logger.info("%d groups, %d instances in %d ms, (remaining %d ms) for query: %s".format(resultSet.numGroups, resultSet.numInstances, resultWatch.elapsedMillis, remainingTime, query))
        resultWatch.reset
        resultWatch.start
        allResults = allResults.combineWith(resultSet)
      }
    }
    return allResults
  }

  def close() = searcherManager.close()
}

object ExtractionGroupFetcher {
  
  val logger = LoggerFactory.getLogger(this.getClass)
  
  val entityStoplistFile = "entity-stoplist-25k.txt"

  val defaultMaxResults = 750

  def loadSearcherManager(path: String, doWarmups: Boolean): SearcherManager = {
    val fsDir = FSDirectory.open(new File(path))
    loadSearcherManager(fsDir, doWarmups)
  }
  
  def loadSearcherManager(dir: Directory, doWarmups: Boolean): SearcherManager = {
    val searcherFactory = if (doWarmups) warmupSearcherFactory else new SearcherFactory()
    val searcherManager = new SearcherManager(dir, searcherFactory)
    logger.info("ExtractionGroupFetcher loading SearcherManager for %s".format(dir))

    val refresher = new Runnable() {
      override def run() = {
        val sleepTime = 1000 * 60 * 60 * 24 // one day
        while (true) {
          try { Thread.sleep(sleepTime) }
          catch { case ie: InterruptedException => ie.printStackTrace }
          logger.info("Refreshing Searcher: %s".format(dir))
          searcherManager.maybeRefresh
        }
      }
    }
    val refresherThread = new Thread(refresher, dir.toString + " refresher")
    refresherThread.setDaemon(true)
    refresherThread.start()
    
    searcherManager
  }
  
  // A searcher factory that runs warmup queries on top of the default implementation. 
  // Doesn't actually materialize the data from disk (but that would be "warming" the IO cache anyway, which doesn't cool off between searchers)
  private val warmupSearcherFactory = new SearcherFactory() {

    override def newSearcher(reader: IndexReader): IndexSearcher = {
      
      val searcher = super.newSearcher(reader)
      QuerySpec.warmupQueries.map(_.luceneQuery).foreach { luceneQuery =>
        logger.info("Running warmup query: %s".format(luceneQuery.toString))
        searcher.search(luceneQuery, TopScoreDocCollector.create(1000, false))
      }
      searcher
    }
  }

  lazy val entityStoplist: Set[String] = {
    val (nsLoad, stopList) = Timing.time {
      using(Source.fromInputStream(this.getClass.getResource(entityStoplistFile).openStream)) { source => source.getLines.toSet}
    }
    logger.info("Loaded entity blacklist of size %d in %s".format(stopList.size, Timing.Milliseconds.format(nsLoad)))
    stopList
  }
}
