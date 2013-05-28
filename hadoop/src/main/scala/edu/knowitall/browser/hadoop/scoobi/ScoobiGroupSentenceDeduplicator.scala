package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import java.net.InetSocketAddress
import java.io.File
import java.io.FileWriter
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.knowitall.common.Timing._
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.EntityLink
import edu.knowitall.browser.entity.Pair
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction
import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction
import scopt.OptionParser
import edu.knowitall.openie.models.util.TaggedStemmer
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput
import edu.knowitall.openie.models.InstanceDeduplicator

/**
  * A mapper job that
  * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
  * then constructs ExtractionGroup[ReVerbExtraction] from the reducer input.
  * linkers is a Seq --- this is because each points to a different lucene index on one of the four of
  * reliable's scratch disks, which helps balance the load, allowing you to run more of these
  * as hadoop map tasks
  *
  * Also adds types - entityTyper does not have to be run as a separate job
  */
object ScoobiGroupSentenceDeduplicator extends ScoobiApp {
  def deduplicateGroups(groups: DList[String]) = {
    groups.map { line =>
      val group = ReVerbExtractionGroup.deserializeFromString(line)
      group match {
        case Some(group) => {
          ReVerbExtractionGroup.serializeToString(InstanceDeduplicator.deduplicate(group))
        }
        case None => throw new IllegalArgumentException("Could not deserialize group: " + group)
      }
    }
  }

  def run() = {
    var inputPath: String = null
    var outputPath: String = null

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups", { str => outputPath = str })
    }

    if (parser.parse(args)) {
      val lines: DList[String] = TextInput.fromTextFile(inputPath)
      val deduplicated: DList[String] = deduplicateGroups(lines)

      persist(TextOutput.toTextFile(deduplicated, outputPath + "/"));
    }
  }
}
