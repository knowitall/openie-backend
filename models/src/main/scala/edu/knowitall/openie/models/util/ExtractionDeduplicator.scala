package edu.knowitall.openie.models.util

import java.util.regex.Pattern

import java.util.Scanner

import scala.collection.mutable

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.openie.models.Extraction

object ExtractionDeduplicator {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val splitPattern = Pattern.compile("[,./?:;\\[\\]\\{\\}|\\\\!@#$%^&*\\(\\-_+=]")

  private val window = 3

  def deduplicate(cluster: ExtractionCluster[Extraction]): ExtractionCluster[Extraction] = {

    if (cluster.instances.size <= 1) return cluster

    // for each sentence, we want the tokens that are:
    // 1- not tokens from the relation tuple
    // 2- within a window of K from the tuple.

    // we keep a sentence only if it contributes a new token, and only if it doesn't repeat too many
    // tokens we've already seen in positions we've seen them in.

    // hack - give wiki extractions a slightly higher confidence in order to prefer while deduping
    def hackConf(inst: Extraction): Double = -(inst.confidence + (if (inst.corpus.equals("wiki")) 0.1 else 0.0))

    val confSorted = cluster.instances.toSeq.sortBy(hackConf _)

    // if a sentence adds a token to one of these, we keep it...
    val tokensSeenBefore = new mutable.HashSet[String]
    val tokensSeenAfter = new mutable.HashSet[String]

    // ... unless it repeats too many tokens we've already seen in the same position as before
    val tokensSeenPositions = new mutable.HashSet[(String, Int)]

    // if the extr covers the whole sentence, keep, but don't duplicate
    val wholeSentExtrs = new mutable.HashSet[String]

    val filteredInstances = confSorted.filter { inst =>

      // get start and end position of window
      val start = Iterable(inst.arg1Interval.start, inst.relInterval.start, inst.arg2Interval.start).min
      val end = Iterable(inst.arg1Interval.end, inst.relInterval.end, inst.arg2Interval.end).max

      // if the instance covers the whole sentence, don't throw it away!
      if (start == 0 && end >= inst.sentenceTokens.length - 2) { // -2 for the trailing period!
        val sentStr = inst.sentenceTokens.map(_.string).mkString(" ").toLowerCase
        val seenBefore = wholeSentExtrs.contains(sentStr)
        if (!seenBefore) wholeSentExtrs.add(sentStr)
        !seenBefore
      } else { // else the extr doesn't cover the sentence, deduplicate according to surrounding tokens

        val sentenceRange = (0 until inst.sentenceTokens.length)

        // get tokens within window
        val tokensBefore = sentenceRange.filter(i => (i < start && i >= start - window)).map(pos => (inst.sentenceTokens(pos).string.toLowerCase, pos))
        val tokensAfter = sentenceRange.filter(i => (i > end && i <= end + window)).map(pos => (inst.sentenceTokens(pos).string.toLowerCase, pos))

        // num repeats in same position as previously seen
        val numRepeats = (tokensBefore ++ tokensAfter).filter(tokensSeenPositions.contains(_)).size
        (tokensBefore ++ tokensAfter).foreach(tokensSeenPositions.add(_))

        if (numRepeats >= window) {
          false
        } else {

          val oldNumSeenBefore = tokensSeenBefore.size
          val oldNumSeenAfter = tokensSeenAfter.size
          tokensBefore.foreach(pair => tokensSeenBefore.add(pair._1))
          tokensAfter.foreach(pair => tokensSeenAfter.add(pair._1))
          (oldNumSeenBefore < tokensSeenBefore.size || oldNumSeenAfter < tokensSeenAfter.size)
        }
      }
    } // END instances.filter BLOCK!

    val deduped = new ExtractionCluster(cluster.arg1, cluster.rel, cluster.arg2, filteredInstances)

    deduped
  }
}
