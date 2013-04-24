package edu.knowitall.browser.lucene

import org.junit.Assert._
import org.scalatest.junit.AssertionsForJUnit

import org.junit.Test
import org.junit.Before
import org.scalatest.Suite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.io.Source
import scala.collection.JavaConversions._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

import org.apache.lucene.document.Document
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.WhitespaceAnalyzer

import edu.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.extraction.ReVerbExtraction
import edu.knowitall.browser.extraction.FreeBaseEntity
import edu.knowitall.browser.extraction.Instance
import edu.knowitall.tool.stem.MorphaStemmer

@RunWith(classOf[JUnitRunner])
class ParallelReVerbIndexBuilderTest extends Suite {

  val numGroupsToTest = 2000

  val parFactor = 4

  var inputLines: Iterator[String] = Source.fromInputStream(this.getClass.getResource("/test-groups-5000.txt").openStream(), "UTF-8").getLines.drop(1000).take(numGroupsToTest)

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtractionGroup.deserializeFromString(e))

  val stemmer = new MorphaStemmer

  @Test
  def testBuildIndex: Unit = {



    val ramDirs = Seq.fill(parFactor)(new RAMDirectory())

    val indexWriters = ramDirs.map(ramDir => new IndexWriter(ramDir, ReVerbIndexBuilder.indexWriterConfig(ramBufferMB=20)))

    val parIndexBuilder = new ParallelIndexBuilder(indexWriters, ReVerbIndexBuilder.inputLineConverter(regroup=false), 100)

    System.err.println("Building parallel test indexes (%s)...".format(parFactor))

    // build the indexes
    parIndexBuilder.indexAll(inputLines)

    indexWriters.foreach(_.close())

    System.err.println("Finished building test indexes, running parallel test queries")

    // open the index and try to read from it
    val searcherManagers = ramDirs.map(ramDir => ExtractionGroupFetcher.loadSearcherManager(ramDir, doWarmups=false))
    val simpleFetchers = searcherManagers.map(new ExtractionGroupFetcher(_, 250, 500, 100000, Set.empty[String]))
    val parFetcher = new ParallelExtractionGroupFetcher(simpleFetchers)

    // test that each input group can be found in the index
    def testGroup(group: ExtractionGroup[ReVerbExtraction]): Unit = {
      val query = QuerySpec.identityQuery(group)
      println(query.luceneQuery)
      val resultGroups = parFetcher.getGroups(query)
      if (!resultGroups.results.toSet.contains(group)) {
        println(); println()
        println("Expected (%d): %s".format(group.instances.size, ReVerbExtractionGroup.serializeToString(group)))

        println("Found: (%d, %d)".format(resultGroups.numGroups, resultGroups.numInstances))
        resultGroups.results.foreach { resultGroup =>
          println(ReVerbExtractionGroup.serializeToString(resultGroup))
        }
        fail()
      }
    }

    getExtrsHelper.foreach(testGroup(_))

    System.err.println("parallel test queries pass")

    parFetcher.close()
  }
}
