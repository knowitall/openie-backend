package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
//import com.nicta.scoobi.DList._
//import com.nicta.scoobi.DList
//import com.nicta.scoobi.io.text.TextInput._
//import com.nicta.scoobi.io.text.TextInput
//import com.nicta.scoobi.io.text.TextOutput._
//import com.nicta.scoobi.io.text.TextOutput

import java.net.InetSocketAddress

import java.io.File
import java.io.FileWriter

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

import edu.knowitall.common.Timing._
import edu.knowitall.browser.extraction.ReVerbExtraction
import edu.knowitall.browser.extraction.FreeBaseEntity
import edu.knowitall.browser.extraction.FreeBaseType
import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.extraction.Instance
import edu.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.knowitall.browser.util.TaggedStemmer
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.EntityLink
import edu.knowitall.browser.entity.Pair
import edu.washington.cs.knowitall.nlp.extraction.ChunkedArgumentExtraction

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

import scopt.OptionParser

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
class ScoobiEntityLinker(val subLinkers: Seq[EntityLinker], val stemmer: TaggedStemmer) {

  import ScoobiEntityLinker.getRandomElement
  import ScoobiEntityLinker.min_arg_length

  private var groupsProcessed = 0
  private var arg1sLinked = 0
  private var arg2sLinked = 0
  private var totalGroups = 0

  def getEntity(el: EntityLinker, arg: String, head: ReVerbExtraction, sources: Set[String]): Option[EntityLink] = {
    if (arg.length < min_arg_length) None
    val tryLink = el.getBestEntity(arg, sources.toSeq)
    if (tryLink == null) None else Some(tryLink)
  }

  def entityConversion(link: EntityLink): (Option[FreeBaseEntity], Set[FreeBaseType]) = {
    val fbEntity = FreeBaseEntity(link.entity.name, link.entity.fbid, link.score, link.inlinks)
    val fbTypes = link.retrieveTypes flatMap FreeBaseType.parse toSet
      
    (Some(fbEntity), fbTypes)
  }

  def linkEntities(reuseLinks: Boolean)(group: ExtractionGroup[ReVerbExtraction]): ExtractionGroup[ReVerbExtraction] = {
    // a hack for the thread problem
    if (groupsProcessed == 0) {
      val keys = Thread.getAllStackTraces.keySet
      System.err.println("Num threads running: " + keys.size)
      keys.foreach { thread => System.err.println("%s, %s, %s".format(thread.getId, thread.getName, thread.getPriority)) }
    }

    groupsProcessed += 1

    val extrs = group.instances.map(_.extraction)

    val head = extrs.head

    val sources = extrs.map(e => e.sentenceTokens.map(_.string).mkString(" "))
    // choose a random linker to distribute the load more evenly across the cluster
    val randomLinker = getRandomElement(subLinkers)

    val (arg1Entity, arg1Types) = if (reuseLinks && group.arg1.entity.isDefined) {
      (group.arg1.entity, group.arg1.types)
    } else {
      val entity = getEntity(randomLinker, head.arg1Head, head, sources) match {
        case Some(rawEntity) => { arg1sLinked += 1; entityConversion(rawEntity) }
        case None => (Option.empty[FreeBaseEntity], Set.empty[FreeBaseType])
      }
      //if (group.arg1.entity.isDefined) require(group.arg1.entity.equals(entity._1))
      entity
    }

    val (arg2Entity, arg2Types) = if (reuseLinks && group.arg2.entity.isDefined) {
      (group.arg2.entity, group.arg2.types)
    } else {
      val entity = getEntity(randomLinker, head.arg2Head, head, sources) match {
        case Some(rawEntity) => { arg2sLinked += 1; entityConversion(rawEntity) }
        case None => (Option.empty[FreeBaseEntity], Set.empty[FreeBaseType])
      }
      //if (group.arg2.entity.isDefined) require(group.arg2.entity.equals(entity._1))
      entity
    }

    val newGroup = new ExtractionGroup(
      group.arg1.norm,
      group.rel.norm,
      group.arg2.norm,
      arg1Entity,
      arg2Entity,
      arg1Types,
      arg2Types,
      group.instances.map(inst => new Instance(inst.extraction, inst.corpus, inst.confidence)))

    newGroup
  }
}

object EntityLinkerStaticVars {
  val linkersLocal = new mutable.HashMap[Thread, ScoobiEntityLinker] with mutable.SynchronizedMap[Thread, ScoobiEntityLinker]
  case class Counter(var count: Int) { def inc(): Unit = { count += 1 } }
  val counterLocal = new ThreadLocal[Counter]() { override def initialValue = Counter(0) }
}

object ScoobiEntityLinker extends ScoobiApp {
  import EntityLinkerStaticVars._
  private val min_arg_length = 3

  val random = new scala.util.Random

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

  def getEntityLinker: ScoobiEntityLinker = getEntityLinker(4)

  def getEntityLinker(num: Int): ScoobiEntityLinker = {
    val el = getScratch(num).map(index => new EntityLinker(index)) // java doesn't have Option
    new ScoobiEntityLinker(el, TaggedStemmer.instance)
  }

  def linkGroups(groups: DList[String], minFreq: Int, maxFreq: Int, reportInterval: Int,
      skipLinking: Boolean): DList[String] = {
    if (skipLinking) return frequencyFilter(groups, minFreq, maxFreq, reportInterval, skipLinking)

    groups.flatMap { line =>
      val counter = counterLocal.get
      counter.inc
      val linker = linkersLocal.getOrElseUpdate(Thread.currentThread, getEntityLinker)
      if (counter.count % reportInterval == 0) {
        val format = "MinFreq: %d, MaxFreq: %d, groups input: %d, groups output: %d, arg1 links: %d, arg2 links: %d"
        System.err.println(format.format(minFreq, maxFreq, counter.count, linker.groupsProcessed, linker.arg1sLinked, linker.arg2sLinked))
      }

      val extrOp = ReVerbExtractionGroup.deserializeFromString(line)
      extrOp match {
        case Some(extr) => {
          if (extr.instances.size <= maxFreq && extr.instances.size >= minFreq) {
            Some(ReVerbExtractionGroup.serializeToString(linker.linkEntities(reuseLinks = false)(extr)))
          } else {
            None
          }
        }
        case None => { System.err.println("ScoobiEntityLinker: Error parsing a group: %s".format(line)); None }
      }
    }
  }

  def frequencyFilter(groups: DList[String], minFreq: Int, maxFreq: Int, reportInterval: Int, skipLinking: Boolean): DList[String] = {

    var groupsOutput = 0

    groups.flatMap { line =>
      val counter = counterLocal.get
      counter.inc
      if (counter.count % reportInterval == 0) {
        val format = "(Skipping Linking) MinFreq: %d, MaxFreq: %d, groups input: %d, groups output: %d"
        System.err.println(format.format(minFreq, maxFreq, counter.count, groupsOutput))
      }

      val extrOp = ReVerbExtractionGroup.deserializeFromString(line)
      extrOp match {
        case Some(extr) => {
          if (extr.instances.size <= maxFreq && extr.instances.size >= minFreq) {
            groupsOutput += 1
            Some(ReVerbExtractionGroup.serializeToString(extr))
          } else {
            None
          }
        }
        case None => { System.err.println("ScoobiEntityLinker: Error parsing a group: %s".format(line)); None }
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
      val lines: DList[String] = TextInput.fromTextFile(inputPath)
      val linkedGroups: DList[String] = linkGroups(lines, minFreq, maxFreq, reportInterval, skipLinking)

      persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
    }
  }
}
