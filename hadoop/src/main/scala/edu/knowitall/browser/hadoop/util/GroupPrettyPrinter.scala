package edu.knowitall.browser.hadoop.util

import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction

import scala.io.Source

object GroupPrettyPrinter {

  lazy val tabHeaders = Seq("arg1", "rel", "arg2", "arg1_entity", "arg2_entity", "sentences...").mkString("\t")

  def getPrettyGroup(group: ExtractionGroup[ReVerbExtraction]): String = {

    val head = group.instances.head.extraction
    val sources = group.instances.map(_.extraction.sentenceTokens).map(sent => sent.map(_.string).mkString(" "))

    val arg1 = head.arg1Tokens.mkString(" ")
    val rel = head.relTokens.mkString(" ")
    val arg2 = head.arg2Tokens.mkString(" ")

    val arg1E = group.arg1.entity match {
      case Some(entity) => (entity.name, entity.fbid).toString
      case None => "none"
    }
    val arg2E = group.arg2.entity match {
      case Some(entity) => (entity.name, entity.fbid).toString
      case None => "none"
    }

    val relFields = Seq(arg1, rel, arg2, arg1E, arg2E)
    val allOutput = relFields ++ sources

    allOutput.mkString("\t")
  }

  def main(args: Array[String]): Unit = {

    println(tabHeaders)

    Source.fromInputStream(System.in).getLines flatMap ReVerbExtractionGroup.deserializeFromString foreach { group =>
      val pretty = getPrettyGroup(group)
      println(pretty)
    }
  }
}