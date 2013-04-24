package edu.knowitall.browser.lucene

import org.apache.lucene.document.Document
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.index.IndexReader
import scala.collection.parallel.ParSeq
import scala.util.Random

import edu.knowitall.openie.models.ReVerbExtractionGroup

/**
 * Prints the contents of an REG index to std out.
 */
class ParallelIndexPrinter(val pegf: ParallelExtractionGroupFetcher) {

  case class DocToFetch(val searcher: IndexSearcher, val docNum: Int) {
    def fetch = searcher.doc(docNum)
    def tryFetch = try { Some(fetch) } catch { case e: Throwable => None }
  }
  
  def getDocuments: Iterator[Document] = {
    val docsToFetch = pegf.simpleFetchers.iterator.map(_.indexSearcher).flatMap { searcher =>
      Iterator.from(0, searcher.getIndexReader().numDocs - 1).map(docNum => DocToFetch(searcher, docNum))  
    }
    
    val grouped = docsToFetch.grouped(pegf.simpleFetchers.size * 50)
    val shuffled = grouped.flatMap { case grp => scala.util.Random.shuffle(grp) }
      
    shuffled.grouped(100).flatMap { grp => grp.toSeq.par.flatMap(_.tryFetch) }
  }
  
  def getRegs = getDocuments map ReVerbDocumentConverter.fromDocument
}

object ParallelIndexPrinter {
  
  import scopt.OptionParser
  
  lazy val defaultInstance = { 
    val pegf = new ParallelExtractionGroupFetcher(
        ParallelExtractionGroupFetcher.defaultIndexes.split(":"),
        // these parameters shouldn't matter, since we go directly to the IndexSearchers
        searchMaxGroups=100, 
        readMaxInstances=100, 
        timeoutMillis=100, 
        stoplist=true)
    
    new ParallelIndexPrinter(pegf)
  }
  
  def main(args: Array[String]): Unit = {
    
    val pegf = new ParallelExtractionGroupFetcher(
        ParallelExtractionGroupFetcher.defaultIndexes.split(":"),
        // these parameters shouldn't matter, since we go directly to the IndexSearchers
        searchMaxGroups=100, 
        readMaxInstances=100, 
        timeoutMillis=100, 
        stoplist=true)
    
    defaultInstance.getRegs map ReVerbExtractionGroup.serializeToString foreach println
  }
}
