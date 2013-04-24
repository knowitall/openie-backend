package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._

import java.io.File
import java.io.FileWriter

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

import edu.knowitall.common.Timing._
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.Pair

import edu.washington.cs.knowitall.nlp.extraction.ChunkedExtraction

object ScoobiReVerbGrouperLinker extends ScoobiApp {

  def run() = {

    val (inputPath, outputPath, corpus) = (args(0), args(1), args(2))

    // serialized ReVerbExtractions
    val extrs: DList[String] = fromTextFile(inputPath)

    // serialized ExtractionGroup[ReVerbExtraction]
    val groups = ScoobiReVerbGrouper.groupExtractions(extrs, corpus)

    val linkedGroups = ScoobiEntityLinker.linkGroups(groups, 0, Integer.MAX_VALUE, 20000, true)

    persist(TextOutput.toTextFile(linkedGroups, outputPath + "/"));
  }
}
