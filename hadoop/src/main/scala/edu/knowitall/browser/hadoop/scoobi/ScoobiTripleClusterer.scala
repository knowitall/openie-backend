package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import java.io.File
import java.io.FileWriter
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.knowitall.common.Timing._
import edu.knowitall.tool.postag.PostaggedToken
import edu.knowitall.tool.stem.MorphaStemmer
import edu.knowitall.tool.postag.Postagger
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

/**
  * A mapper + reducer job that
  * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
  * then constructs ExtractionCluster[ReVerbExtraction] from the reducer input. The Entity Linker
  * code is run in the reducer.
  */
class ScoobiTripleClusterer(val stemmer: TaggedStemmer) {
  final val max_group_size = 40000

  private var extrsProcessed = 0
  private var groupsProcessed = 0

  private var largestGroup = 0

  private final val MAX_GROUP_SIZE = 500000

  def getKeyValuePair(line: String): Option[(String, String)] = try {
    extrsProcessed += 1
    if (extrsProcessed % 20000 == 0) System.err.println("Extractions processed: %d".format(extrsProcessed))

    // parse the line to an Extraction
    implicitly[TabFormat[Extraction]].read(line).toOption.orElse {
      throw new MatchError("Could not deserialize extraction: " + line)
      None
    }.filter(cluster => ScoobiClusterFilter.instanceFilterCondition(0.6)(cluster)).flatMap { extr =>
      val key = extr.indexGroupingKey
      val keyString = "%s__%s__%s".format(key._1, key._2, key._3)

      // don't output if part of the key is empty
      if (key.productIterator.exists(_.asInstanceOf[String].isEmpty)) None
      else Some((keyString, line))
    }
  } catch {
    case e: Exception => { e.printStackTrace; None }
  }

  def processCluster(key: String, rawExtrs: Iterable[String]): Option[ExtractionCluster[Extraction]] = try {
    val rawExtrsTruncated = rawExtrs.take(max_group_size)
    if (rawExtrs.size > MAX_GROUP_SIZE) None
    else {
      groupsProcessed += 1
      if (groupsProcessed % 10000 == 0) System.err.println("Groups processed: %d, current key: %s, largest group: %d".format(groupsProcessed, key, largestGroup))

      // deserialize extraction strings
      val extrs = rawExtrsTruncated.flatMap(line => implicitly[TabFormat[Extraction]].read(line).toOption)
      if (extrs.size > largestGroup) largestGroup = extrs.size

      // build a normalized key from the first extraction
      val head = extrs.head
      val normKey = head.indexGroupingKeyString
      val normTuple = head.indexGroupingKey
      // make sure it's the same as the key
      require(normKey.equals(key), "Keys do not match: " + normKey + " != " + key)

      // build the sources list
      val sources = extrs.map(e => e.sentenceTokens.map(_.string).mkString(" "))

      // no entity since we have not linked yet
      val arg1Entity = None
      val arg2Entity = None

      val newGroup = new ExtractionCluster(
        normTuple._1,
        normTuple._2,
        normTuple._3,
        arg1Entity,
        arg2Entity,
        Set.empty[FreeBaseType],
        Set.empty[FreeBaseType],
        extrs.toSeq)

      Some(newGroup)
    }
  } catch {
    case e: Exception => {System.err.println("Exception on key: " + key); e.printStackTrace; None }
  }
}

object ScoobiTripleGrouper extends ScoobiApp {
  /** extrs --> grouped by normalization key */
  def groupExtractions(extrs: DList[String]): DList[String] = {
    lazy val grouper = new ScoobiTripleClusterer(TaggedStemmer)
    val keyValuePair: DList[(String, String)] = extrs.mapFlatten { line =>
      grouper.getKeyValuePair(line)
    }

    keyValuePair.groupByKey.mapFlatten {
      case (key, sources) =>
        grouper.processCluster(key, sources).filter(_.instances.size > 1).map { cluster =>
          implicitly[TabFormat[ExtractionCluster[Extraction]]].write(ExtractionDeduplicator.deduplicate(cluster))
        }
    }
  }

  def run() = {
    val (inputPath, outputPath) = (args(0), args(1))

    // serialized ReVerbExtractions
    val extrs: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    val groups = groupExtractions(extrs)

    persist(TextOutput.toTextFile(groups, outputPath + "/"));
  }
}
