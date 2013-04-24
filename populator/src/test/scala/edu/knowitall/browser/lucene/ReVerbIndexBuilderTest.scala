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

import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.Instance
import edu.knowitall.tool.stem.MorphaStemmer


@RunWith(classOf[JUnitRunner])
class ReVerbIndexBuilderTest extends Suite {

  val numGroupsToTest = 2000

  var inputLines: List[String] = Source.fromInputStream(this.getClass.getResource("/test-groups-5000.txt").openStream(), "UTF-8").getLines.drop(1000).take(numGroupsToTest).toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtractionGroup.deserializeFromString(e))

  val stemmer = new MorphaStemmer

  @Test
  def testBuildIndex: Unit = {



    val ramDir = new RAMDirectory()

    val indexWriter = new IndexWriter(ramDir, ReVerbIndexBuilder.indexWriterConfig(ramBufferMB=10))

    val indexBuilder = new IndexBuilder(indexWriter, ReVerbIndexBuilder.inputLineConverter(regroup=false), 100)

    System.err.println("Building test index...")

    // build the index
    indexBuilder.indexAll(inputLines.iterator)

    indexWriter.close

    System.err.println("Finished building test index, running test queries")

    // open the index and try to read from it
    val searcherManager = ExtractionGroupFetcher.loadSearcherManager(ramDir, doWarmups=false)
    val fetcher = new ExtractionGroupFetcher(searcherManager, 1000, 1000, 10000, Set.empty[String])


  // test that each input group can be found in the index
    def testGroup(group: ExtractionGroup[ReVerbExtraction]): Unit = {
      val query = QuerySpec.identityQuery(group)
      println(query.luceneQuery)
      val resultGroups = fetcher.getGroups(query)
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

    System.err.println("Test queries pass")

    ramDir.close
    searcherManager.close
  }
}
