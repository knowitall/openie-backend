package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import java.io.File
import java.io.FileWriter
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.knowitall.common.Timing._
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.Pair
import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction
import edu.knowitall.openie.models.util.TaggedStemmer
import com.nicta.scoobi.io.text.TextOutput
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat
import edu.knowitall.openie.models.TripleExtraction
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.openie.models.serialize.TabFormat
import edu.knowitall.openie.models.Extraction
import edu.knowitall.openie.models.util.ExtractionDeduplicator
import scala.util.{ Success, Failure, Try }

object ScoobiTripleGroupDeduplicator extends ScoobiApp {
  def run() = {
    val (inputPath, outputPath) = (args(0), args(1))

    // serialized ReVerbExtractions
    val lines: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    val deduplicated = lines.mapFlatten { line =>
      ExtractionCluster.TabFormat.read(line) match {
        case Failure(e) =>
          System.err.println("Could not deserialize line: " + line)
          e.printStackTrace
          None
        case Success(cluster) =>
          Some(ExtractionCluster.TabFormat.write(ExtractionDeduplicator.deduplicate(cluster)))
      }
    }

    persist(TextOutput.toTextFile(deduplicated, outputPath + "/"));
  }
}
