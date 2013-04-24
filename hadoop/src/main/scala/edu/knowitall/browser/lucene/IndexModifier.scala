package edu.knowitall.browser.lucene

import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction

import org.apache.lucene.search.SearcherManager
import org.apache.lucene.search.SearcherFactory

import edu.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker

import org.apache.lucene.search.IndexSearcher

abstract class IndexModifier {
  type REG = ExtractionGroup[ReVerbExtraction]

  def updateAll(groups: Iterator[ExtractionGroup[ReVerbExtraction]]): Unit

  def fetcher: GroupFetcher

  def close(): Unit
}

/**
  * Adds collections of unlinked ReVerb ExtractionGroups to an existing index,
  * such as an index created by IndexBuilder.
  *
  * Command-line interface allows the user to skip linking for singleton groups
  * that do not join any new group in the index (this saves a lot of time)
  */
class ReVerbIndexModifier(
  val writer: IndexWriter,
  val linker: Option[ScoobiEntityLinker],
  val writerBufferMb: Int,
  val groupsPerCommit: Int) extends IndexModifier {

  val searcherManager = new SearcherManager(writer, true, new SearcherFactory())
  val privateFetcher = new ExtractionGroupFetcher(searcherManager, 1000, 1000, 10000, Set.empty[String])
  def fetcher = { searcherManager.maybeRefresh; privateFetcher }
  private val searcher = fetcher.indexSearcher
  private val reader = fetcher.indexSearcher.getIndexReader

  /**
    * Updates group to the index. Returns true if the index was modified.
    * User can specify to only add if the group is already in the index
    * (will be useful for parallel indexes)
    */
  protected[lucene] def updateGroup(group: REG, onlyIfAlreadyExists: Boolean): Boolean = {

    val querySpec = QuerySpec.identityQuery(group)

    val queryGroups = fetcher.getGroups(querySpec) match {
      case Timeout(results, _) => throw new RuntimeException("Failed to add document due to timeout.")
      case Limited(results, _) => throw new RuntimeException("Index results were limited... this shouldn't happen!")
      case Success(results) => results
    }

    if (!onlyIfAlreadyExists || !queryGroups.isEmpty) {

      // delete matching documents from the index
      writer.deleteDocuments(querySpec.luceneQuery)

      val newKey = group.instances.head.extraction.indexGroupingKey

      // queryGroups contains a superset of what we want - group by key to find the ones we need to merge
      val keyedGroups = (group :: queryGroups).groupBy(_.instances.head.extraction.indexGroupingKey)

      val mergedGroups = keyedGroups.map { case (key, groups) => ReVerbExtractionGroup.mergeGroups(key, groups) }

      mergedGroups foreach { mergedGroup =>
        val updateLink = (mergedGroup.instances.head.extraction.indexGroupingKey.equals(newKey))
        addGroup(mergedGroup, updateLink)
      }

      return true
    } else {
      false
    }
  }

  def addGroup(group: REG, tryLink: Boolean): Unit = {
    val size = group.instances.size

    var linkedGroup = group

    // re-run the linker only if the right conditions hold
    if (size > 1 && (size < 5 || size % 2 == 0)) {

      if (tryLink) {
        linkedGroup = linker match {
          case Some(scoobiLinker) => scoobiLinker.linkEntities(reuseLinks = false)(group)
          case None => linkedGroup
        }
      } else linkedGroup
    }

    val document = ReVerbDocumentConverter.toDocument(linkedGroup)
    println("Adding: " + ReVerbExtractionGroup.serializeToString(group))

    writer.addDocument(document)
  }

  def updateAll(groups: Iterator[REG]): Unit = {

    var groupsProcessed = 0

    groups.grouped(groupsPerCommit).foreach { groupOfGroups =>
      groupOfGroups.foreach(updateGroup(_, false))
      groupsProcessed += groupOfGroups.size
      writer.commit()
      System.err.println("Groups inserted: %d Index MaxDoc: %d".format(groupsProcessed, writer.maxDoc))

    }
  }

  def close(): Unit = {
    writer.close
  }
}
