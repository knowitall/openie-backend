package edu.knowitall.browser.lucene

import java.io.File

import scala.io.Source

import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.WhitespaceAnalyzer

import edu.knowitall.openie.models.ReVerbExtractionGroup

import scopt.OptionParser



/**
 * An index builder creates new indexes and appends to existing indexes, but does not modify existing documents within an index.
 */
class IndexBuilder(
    val indexWriter: IndexWriter,
    val inputLineConverter: String => Iterable[Document],
    val linesPerCommit: Int) {

  var linesIndexed = 0
  var groupsIndexed = 0

  private def indexLines(input: Iterable[String]): Unit = {

    input.flatMap { line => 
      
      linesIndexed += 1
      inputLineConverter(line) 
    }.foreach { doc =>
      groupsIndexed += 1
      indexWriter.addDocument(doc)
    }
  }

  private def indexAndCommit(input: Iterable[String]): Unit = {

    indexLines(input)
    indexWriter.commit
    indexWriter.message("Lines indexed: %s, Groups indexed: %s".format(linesIndexed.toString, groupsIndexed.toString))
  }


  def indexAll(input: Iterator[String]): Unit = {

    input.grouped(linesPerCommit).foreach(group => indexAndCommit(group))
  }
}

object ReVerbIndexBuilder {

  var linesPerCommit = 250000
  var ramBufferMB = 1000 // passed to indexWriterConfig.
  val tabSplitter = "\t".r

  def indexWriterConfig(ramBufferMB: Int) = {
    val analyzer = new WhitespaceAnalyzer(Version.LUCENE_36)
    val conf = new IndexWriterConfig(Version.LUCENE_36, analyzer)

    conf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH) // the implementation commits every linesPerCommit
    conf.setRAMBufferSizeMB(ramBufferMB)
    conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

    conf
  }

  private def regroupingInputLineConverter(line: String): Iterable[Document] = {
    try {
      val groupOption = ReVerbExtractionGroup.deserializeFromString(line)
      groupOption match {
        case Some(group) => group.reNormalize.map(newGroup=>ReVerbDocumentConverter.toDocument(newGroup))
        case None => Set.empty
      }
    } catch {
      case iae: IllegalArgumentException => {
        System.err.println("regroupingInputLineConverter error. Input:%s".format(line))
        Set.empty
      }
    }
  }
  
  private def standardInputLineConverter(line: String): Iterable[Document] = {
    val groupOption = ReVerbExtractionGroup.deserializeFromString(line)
    groupOption.map(group => ReVerbDocumentConverter.toDocument(group))
  }

  def inputLineConverter(regroup: Boolean) = regroup match {
    case true => regroupingInputLineConverter _
    case false => standardInputLineConverter _
  }
  
  def main(args: Array[String]): Unit = {

    var indexPath = ""
    var regroup = false

    val optionParser = new OptionParser() {
      arg("path", "Path to index", { str => indexPath = str })
      opt("linesPerCommit", "Lines added between each IndexWriter.commit", { str => linesPerCommit = str.toInt })
      opt("ramBufferMB", "Size of ram buffer, (e.g. IndexWriterConfig.setRAMBufferSizeMB)", { str => ramBufferMB = str.toInt })
      opt("regroup", "For each group individually re-group based on the current index grouping key", { regroup = true })
    }

    // bail if the args are bad
    if (!optionParser.parse(args)) return

    val indexWriter = new IndexWriter(FSDirectory.open(new File(indexPath)), indexWriterConfig(ramBufferMB))
    indexWriter.setInfoStream(System.err)

    val lineConverter = inputLineConverter(regroup)
    
    val indexBuilder = new IndexBuilder(indexWriter, lineConverter, linesPerCommit)

    indexBuilder.indexAll(Source.fromInputStream(System.in).getLines)

    indexWriter.close
  }
}

