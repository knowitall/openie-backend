package edu.knowitall.browser.lucene

import java.io.File

import scala.io.Source
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Document
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.WhitespaceAnalyzer

import edu.knowitall.openie.models._

import scopt.OptionParser

/**
 * Note that this implementation doesn't contain any explicit parallelism - we rely instead
 * on parallelism within indexWriters and within the OS. Doing it this way should make it easier
 * to reason about how to proceed in the event that this indexer crashes after indexing k lines.
 *
 * For example, you can just skip k lines and start up this process again. If everything was
 * actually in a separate thread, it would be more difficult to resume each separate thread where
 * it left off.
 */
class ParallelIndexBuilder(
    val indexWriters: Seq[IndexWriter],
    val inputLineConverter: String => Iterable[Document],
    val linesPerCommit: Int) {

  var linesIndexed = 0
  var groupsIndexed = 0
  private def indexLines(input: Iterable[String]): Unit = {

    input.par.flatMap{ line =>
      linesIndexed += 1
      inputLineConverter(line)
    }.toStream.grouped(indexWriters.size).foreach { docs =>
      indexWriters.zip(docs).foreach { case (indexWriter, doc) =>
        indexWriter.addDocument(doc)
        groupsIndexed += 1
      }
    }
  }

  private def indexAndCommit(input: Iterable[String]): Unit = {

    indexLines(input)
    indexWriters.map(writer => future { writer.commit() }).foreach(Await.result(_, 60 seconds))
    System.err.println("Lines indexed: %s, Groups indexed: %s".format(linesIndexed.toString, groupsIndexed.toString))
  }

  def indexAll(input: Iterator[String]): Unit = {

    input.grouped(linesPerCommit).foreach(group => indexAndCommit(group))
  }

  def close() = indexWriters.map(writer => future(writer.close())).foreach(Await.result(_, 60 seconds))
}

object ReVerbParallelIndexBuilder {

  var linesPerCommit = 500000 // across all indexes
  var ramBufferMB = 500 // per index
  val tabSplitter = "\t".r

  def main(args: Array[String]): Unit = {

    var indexPaths: Seq[String] = Nil
    var regroup = false
    var instanceLimit = -1;

    val optionParser = new OptionParser() {
      arg("indexPaths", "Colon-delimited list of paths to indexes", { str => indexPaths = str.split(":") })
      opt("linesPerCommit", "Lines added across all indexes between commits", { str => linesPerCommit = str.toInt })
      opt("ramBufferMB", "Size of ram buffer per index, (e.g. IndexWriterConfig.setRAMBufferSizeMB)", { str => ramBufferMB = str.toInt })
      opt("regroup", "For each group individually re-group based on the current index grouping key", { regroup = true })
      intOpt("instanceLimit", "Limit the number of instances in each REG, mutex regroup option", { i => instanceLimit = i })
    }

    // bail if the args are bad
    if (!optionParser.parse(args)) return

    val indexWriters = indexPaths.map { indexPath =>
      val indexWriter = new IndexWriter(FSDirectory.open(new File(indexPath)), ReVerbIndexBuilder.indexWriterConfig(ramBufferMB))
      indexWriter.setInfoStream(System.err)
      indexWriter
    }

    val converter = if (instanceLimit == -1) {
      ReVerbIndexBuilder.inputLineConverter(regroup)
    } else {
      (groupString: String) => {
        val groupOpt = ReVerbExtractionGroup.deserializeFromString(groupString)
        val groupLimited = groupOpt.map(group => group.copy(instances = group.instances.take(instanceLimit)))
        groupLimited map ReVerbDocumentConverter.toDocument
      }
    }

    val parIndexBuilder = new ParallelIndexBuilder(indexWriters, ReVerbIndexBuilder.inputLineConverter(regroup), linesPerCommit)

    parIndexBuilder.indexAll(Source.fromInputStream(System.in).getLines)

    parIndexBuilder.close()
  }

}