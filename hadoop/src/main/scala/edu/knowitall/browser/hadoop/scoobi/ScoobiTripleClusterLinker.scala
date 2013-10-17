package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import java.net.InetSocketAddress
import java.io.File
import java.io.FileWriter
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.knowitall.common.Timing._
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.openie.models.Extraction
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.Instance
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.EntityLink
import edu.knowitall.browser.entity.Pair
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction
import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction
import scopt.OptionParser
import edu.knowitall.openie.models.util.TaggedStemmer
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextOutput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat
import edu.knowitall.tool.postag.PostaggedToken
import edu.knowitall.browser.entity.util.HeadPhraseFinder
import edu.knowitall.browser.entity.CrosswikisCandidateFinder

/**
  * A mapper job that
  * takes tab-delimited ReVerbExtractions as input, groups them by a normalization key, and
  * then constructs ExtractionCluster[Extraction] from the reducer input.
  * linkers is a Seq --- this is because each points to a different lucene index on one of the four of
  * reliable's scratch disks, which helps balance the load, allowing you to run more of these
  * as hadoop map tasks
  *
  * Also adds types - entityTyper does not have to be run as a separate job
  */
class ScoobiTripleClusterLinker(val subLinkers: Seq[EntityLinker], val stemmer: TaggedStemmer) {

  import ScoobiTripleClusterLinker.getRandomElement
  import ScoobiTripleClusterLinker.min_arg_length

  private var groupsProcessed: Int = 0
  private var arg1sLinked: Int = 0
  private var arg2sLinked: Int = 0
  private var totalGroups: Int = 0

  def getEntity(el: EntityLinker, arg: Seq[PostaggedToken], sources: Set[String]): Option[EntityLink] = {
    if (arg.length < min_arg_length) None
    val headPhrase = HeadPhraseFinder.getHeadPhrase(arg, el.candidateFinder)
    el.getBestEntity(headPhrase, sources.toSeq)
  }

  def entityConversion(link: EntityLink): (Option[FreeBaseEntity], Set[FreeBaseType]) = {
    val fbEntity = FreeBaseEntity(link.entity.name, link.entity.fbid, link.combinedScore, link.inlinks)
    val fbTypes = link.retrieveTypes flatMap FreeBaseType.parse toSet

    (Some(fbEntity), fbTypes)
  }

  def linkEntities(reuseLinks: Boolean)(group: ExtractionCluster[Extraction]): ExtractionCluster[Extraction] = {
    groupsProcessed += 1

    val extrs = group.instances

    val head = extrs.head

    val sources = extrs.map(e => e.sentenceTokens.map(_.string).mkString(" "))
    // choose a random linker to distribute the load more evenly across the cluster
    val randomLinker = getRandomElement(subLinkers)

    val (arg1Entity, arg1Types) = if (reuseLinks && group.arg1.entity.isDefined) {
      (group.arg1.entity, group.arg1.types)
    } else {
      val entity = getEntity(randomLinker, head.arg1Tokens, sources.toSet) match {
        case Some(rawEntity) => { arg1sLinked += 1; entityConversion(rawEntity) }
        case None => (Option.empty[FreeBaseEntity], Set.empty[FreeBaseType])
      }
      entity
    }

    val (arg2Entity, arg2Types) = if (reuseLinks && group.arg2.entity.isDefined) {
      (group.arg2.entity, group.arg2.types)
    } else {
      val entity = getEntity(randomLinker, head.arg2Tokens, sources.toSet) match {
        case Some(rawEntity) => { arg2sLinked += 1; entityConversion(rawEntity) }
        case None => (Option.empty[FreeBaseEntity], Set.empty[FreeBaseType])
      }
      entity
    }

    val newGroup = new ExtractionCluster[Extraction](
      group.arg1.norm,
      group.rel.norm,
      group.arg2.norm,
      arg1Entity,
      arg2Entity,
      arg1Types,
      arg2Types,
      group.instances)

    newGroup
  }
}

object ScoobiTripleClusterLinker extends ScoobiApp {
  import EntityLinkerStaticVars._
  private val min_arg_length = 3

  lazy val linker = getEntityLinker(4)

  // hardcoded for the rv cluster - the location of Tom's freebase context similarity index.
  // Indexes are on the /scratchX/ where X in {"", 2, 3, 4}, the method getScratch currently
  // decides how to pick one of the choices.
  /** Get a random scratch directory on an RV node. */
  def getScratch(num: Int): Seq[String] = {
    for (i <- 1 to num) yield {
      val numStr = if (i == 1) "" else i.toString
      "/scratch%s/".format(numStr)
    }
  }

  def getRandomElement[T](seq: Seq[T]): T = seq(Random.nextInt(seq.size))

  def getEntityLinker(num: Int): ScoobiTripleClusterLinker = {
    val el = getScratch(num).map(index => {
      new EntityLinker(new File(index))
    }) // java doesn't have Option
    new ScoobiTripleClusterLinker(el, TaggedStemmer)
  }

  def linkGroups(groups: DList[String], minFreq: Int, maxFreq: Int, reportInterval: Int,
      skipLinking: Boolean): DList[String] = {
    if (skipLinking) return frequencyFilter(groups, minFreq, maxFreq, reportInterval, skipLinking)

    groups.flatMap { line =>
      if (linker.groupsProcessed % 1000 == 0) {
        val format = s"MinFreq: $minFreq, MaxFreq: $maxFreq, groups input: ${linker.groupsProcessed}, arg1 links: ${linker.arg2sLinked}, arg2 links: ${linker.arg2sLinked}"
        System.err.println(format)
      }

      val extrOp = ExtractionCluster.TabFormat.read(line).toOption
      extrOp match {
        case Some(extr) if extr.instances.size > 0 => {
          if (extr.instances.size <= maxFreq && extr.instances.size >= minFreq) {
            Some(ExtractionCluster.TabFormat.write(linker.linkEntities(reuseLinks = false)(extr)))
          } else {
            System.err.println("Trouble deserializing: " + line)
            None
          }
        }
        case None => { System.err.println("ScoobiTripleGroupLinker: Error parsing a group: %s".format(line)); None }
      }
    }
  }

  def frequencyFilter(groups: DList[String], minFreq: Int, maxFreq: Int, reportInterval: Int, skipLinking: Boolean): DList[String] = {

    var groupsOutput = 0

    groups.mapFlatten { line =>
      /*
        val format = "(Skipping Linking) MinFreq: %d, MaxFreq: %d, groups input: %d, groups output: %d"
        System.err.println(format.format(minFreq, maxFreq, counter.count, groupsOutput))
        */

      val extrOp = ExtractionCluster.formatter.read(line).toOption
      extrOp match {
        case Some(extr) => {
          if (extr.instances.size <= maxFreq && extr.instances.size >= minFreq) {
            groupsOutput += 1
            Some(ExtractionCluster.formatter.write(extr))
          } else {
            None
          }
        }
        case None => { System.err.println("ScoobiTripleGroupLinker: Error parsing a group: %s".format(line)); None }
      }
    }
  }

  def run() = {

    var minFreq = 0
    var maxFreq = scala.Int.MaxValue
    var inputPath, outputPath = ""
    var reportInterval = 20000
    var skipLinking = false;

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path, tab delimited ExtractionGroups", { str => inputPath = str })
      arg("outputPath", "hdfs output path, tab delimited ExtractionGroups", { str => outputPath = str })
      opt("minFreq", "minimum num instances in a group to process it inclusive default 0", { str => minFreq = str.toInt })
      opt("maxFreq", "maximum num instances in a group to process it inclusive default Int.MaxValue", { str => maxFreq = str.toInt })
      opt("reportInterval", "print simple stats every n input groups default 20000", { str => reportInterval = str.toInt })
      opt("skipLinking", "don't ever actually try to link - (use for frequency filtering)", { skipLinking = true })
    }

    if (parser.parse(args)) {
      val lines: DList[String] = TextInput.fromTextSource(
        new TextSource(
          Seq(inputPath),
          inputFormat = classOf[LzoTextInputFormat]))
      val linkedGroups: DList[String] = linkGroups(lines, minFreq, maxFreq, reportInterval, skipLinking)

      persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
    }
  }
}
