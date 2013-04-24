package edu.knowitall.browser.lucene.benchmark

import scala.io.Source
import scala.util.Random

import scopt.OptionParser

import edu.knowitall.browser.lucene._
import edu.knowitall.common.Timing.{ Seconds, Milliseconds }
import edu.knowitall.common.Timing

import org.slf4j.LoggerFactory

/**
  * Runs a set of benchmark queries and reports timing data.
  *
  * Before any benchmark queries are run, a warm-up query is
  * issued (see wiki.apache.org/lucene-java/ImproveSearchingSpeed)
  *
  * Single/Multiple indexes are specified in the benchmark config
  * file, by 'overloading' the ReVerb-G1b and R2A2-G1b index paths.
  *
  * @author Rob
  */
class IndexBenchmarker(val fetcher: GroupFetcher, numQueries: Int) {

  def println(str: String):Unit = { System.out.println("Benchmark: %s".format(str)) }

  abstract class SomeTime { def time: Long }
  case class SearchTime(val time: Long) extends SomeTime { override def toString = Seconds.format(time) }

  import IndexBenchmarker.{ fieldMasks, commonNouns, commonVerbs }

  /** Runs the benchmark queries over the specified "corpus" */
  def runResults(): Iterator[(QuerySpec, ResultSet, SearchTime)] = {

    println("Running Warm-up Queries...")
    println("Time\tGroups\tInstances\tTotalHits\tQuery Status")
    fetcher.getGroups(arg1 = Some("Bill"), arg2 = Some("Microsoft"))
    fetcher.getGroups(rel = Some("conjugate"))
    fetcher.getGroups(arg1 = Some("Etzioni"))

    randomQueries(numQueries).iterator.map { querySpec =>
      val (searchTime, resultSet) = Timing.time(fetcher.getGroups(querySpec))
      (querySpec, resultSet, SearchTime(searchTime))
    }
  }

  def printResults: Unit = {
    val resultsIterator = runResults()
    val results = resultsIterator.map { case (querySpec, resultSet, searchTime) =>

      val totalHits = resultSet match{
        case Success(_) => resultSet.numGroups
        case Limited(_, hits) => hits
        case Timeout(_, hits) => hits
      }

      def resultString(prefix:String) = "%s\t%d\t%d\t%d\t%s".format(searchTime, resultSet.numGroups, resultSet.numInstances, totalHits, prefix+queryTitle(querySpec))

      resultSet match {
        case Success(_) => println(resultString("SUCCESS"))
        case Limited(_, _) => println(resultString("LIMITED"))
        case Timeout(_, _) => println(resultString("TIMEOUT"))
      }
      (querySpec, resultSet, SearchTime(searchTime.time))
    } toSeq

    def sumTimes(times:Iterable[SomeTime]): SearchTime = SearchTime(times.map(_.time).sum)
    def avgTimes(times:Iterable[SomeTime]): SearchTime = SearchTime(sumTimes(times).time/times.size)

    val allSearchTime = sumTimes(results.map(_._3)).toString
    val avgSearchTime = avgTimes(results.map(_._3)).toString
    println("Tot Search Time: %s".format(allSearchTime))
    println("Avg Search Time: %s".format(avgSearchTime))
  }

  private def randomQueries(numQueries: Int): Seq[QuerySpec] = {

    val nq = numQueries / fieldMasks.length

    fieldMasks.flatMap {
      case (arg1Mask, relMask, arg2Mask) =>
        Seq.fill(nq) { randomQuery(arg1Mask, relMask, arg2Mask) }
    }
  }

  private def queryTitle(qs: QuerySpec): String = {

    val arg1Str = qs.arg1.getOrElse("___")
    val relStr = qs.rel.getOrElse("___")
    val arg2Str = qs.arg2.getOrElse("___")

    "(%s, %s, %s)".format(arg1Str, relStr, arg2Str)
  }

  private def randomQuery(arg1: Boolean, rel: Boolean, arg2: Boolean): QuerySpec = {

    val arg1Op = if (arg1) Some(randomCommonNoun()) else None
    val relOp = if (rel) Some(randomCommonVerb()) else None
    val arg2Op = if (arg2) Some(randomCommonNoun()) else None

    QuerySpec(arg1Op, relOp, arg2Op, None, None, None, None)
  }

  private def randomCommonNoun(): String = commonNouns(Random.nextInt(commonNouns.length))

  private def randomCommonVerb(): String = commonVerbs(Random.nextInt(commonVerbs.length))
}

object IndexBenchmarker {

  import edu.knowitall.common.Resource.using

  private val COMMON_NOUNS = "/common-nouns.txt"

  private val COMMON_VERBS = "/common-verbs.txt"

  private lazy val commonNouns: IndexedSeq[String] = using(Source.fromInputStream(this.getClass.getResource(COMMON_NOUNS).openStream)) { source =>
    source.getLines.toList.toIndexedSeq
  }

  private lazy val commonVerbs: IndexedSeq[String] = using(Source.fromInputStream(this.getClass.getResource(COMMON_VERBS).openStream)) { source =>
    source.getLines.toList.toIndexedSeq
  }

  private val fieldMasks = Seq(
    (false, true, true),
    (true, false, true),
    (true, true, false),
    (true, false, false),
    (false, true, false),
    (false, false, true))

  def main(args: Array[String]): Unit = {

    var fetcherIndexPath = ""
    var numQueries = -1
    var timeout = -1
    var maxGroups = -1
    var maxInstances = -1

    val parser = new OptionParser() {
      arg("indexPath", "Path to single index", {str=>fetcherIndexPath=str})
      arg("numQueries", "Number of random queries to run (make it divisible by 6)", {str=>numQueries=str.toInt})

      arg("maxGroups", "maximum groups", {str=>maxGroups=str.toInt})
      arg("maxInstances", "maximum instances", {str=>maxInstances=str.toInt})

      arg("timeout", "timeout in milliseconds", {str=>timeout=str.toInt})
    }

    if (!parser.parse(args)) return

    new IndexBenchmarker(new ExtractionGroupFetcher(fetcherIndexPath, maxGroups, maxInstances, timeout), numQueries).printResults
  }
}

object ParallelIndexBenchmarker {

  import IndexBenchmarker.COMMON_NOUNS
  import IndexBenchmarker.COMMON_VERBS
  import IndexBenchmarker.fieldMasks

  def main(args: Array[String]): Unit = {

    var fetcherIndexPaths = Seq.empty[String]
    var numQueries = -1
    var timeout = -1
    var maxGroups = -1
    var maxInstances = -1

    val parser = new OptionParser() {
      arg("indexPaths", "Paths to mutliple indexes, colon delimited", {str=>fetcherIndexPaths=str.split(":")})
      arg("numQueries", "Number of random queries to run (make it divisible by 6)", {str=>numQueries=str.toInt})

      arg("maxGroups", "maximum groups", {str=>maxGroups=str.toInt})
      arg("maxInstances", "maximum instances", {str=>maxInstances=str.toInt})

      arg("timeout", "timeout in milliseconds", {str=>timeout=str.toInt})
    }

    if (!parser.parse(args)) return

    new IndexBenchmarker(new ParallelExtractionGroupFetcher(fetcherIndexPaths, maxGroups, maxInstances, timeout), numQueries).printResults
  }
}
