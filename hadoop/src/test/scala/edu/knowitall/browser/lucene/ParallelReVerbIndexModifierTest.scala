package edu.knowitall.browser.lucene

import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.tool.stem.MorphaStemmer
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.SearcherManager
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.store.RAMDirectory
import org.junit.runner.RunWith
import org.junit.Test
import org.scalatest.junit.JUnitRunner
import org.scalatest.Suite
import scala.Option.option2Iterable
import scala.io.Source
import edu.knowitall.openie.models.Resources

@RunWith(classOf[JUnitRunner])
class ParallelReVerbIndexModifierTest extends Suite {

  type REG = ExtractionGroup[ReVerbExtraction]

  val numGroupsToTest = 1000

  val numIndexes = 7

  val rawInputLines: List[String] = Source.fromInputStream(Resources.groupsUrl.openStream(), "UTF-8").getLines.drop(1000).take(numGroupsToTest).toList

  val inputLines = rawInputLines flatMap ReVerbExtractionGroup.deserializeFromString flatMap (_.reNormalize) map ReVerbExtractionGroup.serializeToString

  val stemmer = new MorphaStemmer

  @Test
  def testModifyIndex: Unit = {

    val ramDirs = (1 to numIndexes) map { _ => new RAMDirectory }

    var indexWriters = ramDirs map { new IndexWriter(_, ReVerbIndexBuilder.indexWriterConfig(ramBufferMB = 10)) }

    val indexBuilder = new ParallelIndexBuilder(indexWriters, ReVerbIndexBuilder.inputLineConverter(regroup = false), 100)

    val eachHalfSize = numGroupsToTest / 2

    println("Halfsize: %d".format(eachHalfSize))

    val randomizedLines = inputLines //scala.util.Random.shuffle(inputLines)
    val firstHalfLines = randomizedLines.take(eachHalfSize)
    val secondHalfLines = randomizedLines.drop(eachHalfSize)

    firstHalfLines foreach println
    println
    secondHalfLines foreach println

    val firstHalfGroups = firstHalfLines flatMap ReVerbExtractionGroup.deserializeFromString
    val secondHalfGroups = secondHalfLines flatMap ReVerbExtractionGroup.deserializeFromString

    System.err.println("Building first half of index:")

    // build the index
    indexBuilder.indexAll(firstHalfLines.iterator)
    var indexReaders = indexWriters map { IndexReader.open(_, true) }
    System.err.println("Finished building first half (%d), adding second half...".format(indexReaders map (_.maxDoc) sum))

    var simpleFetchers = indexWriters map { writer =>
      val searcherManager = new SearcherManager(writer, true, new SearcherFactory())
      new ExtractionGroupFetcher(searcherManager, 10000, 10000, 100000, Set.empty[String])
    }
    var parFetcher = new ParallelExtractionGroupFetcher(simpleFetchers)

    testAll(parFetcher, firstHalfGroups)

    val indexModifiers = indexWriters map { indexWriter => new ReVerbIndexModifier(indexWriter, None, 100, 100) }
    val parModifier = new ParallelReVerbIndexModifier(indexModifiers, 100)

    parModifier.updateAll(secondHalfGroups.iterator)

    System.err.println("Finished building second half (%d)".format(indexReaders map (_.maxDoc) sum))

    testAll(parModifier.fetcher, secondHalfGroups)
    testAll(parModifier.fetcher, firstHalfGroups)
    parModifier.close
  }

  def testAll(fetcher: GroupFetcher, groups: Iterable[REG]): Unit = { groups.foreach(testGroup(fetcher, _)) }

  // test that each input group can be found in the index
  def testGroup(fetcher: GroupFetcher, group: ExtractionGroup[ReVerbExtraction]): Unit = {
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
}
