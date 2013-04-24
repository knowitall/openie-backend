package edu.knowitall.browser.entity.util

import scala.io.Source

import edu.knowitall.common.Resource.using

import edu.knowitall.browser.entity.EntityLink
import edu.knowitall.browser.entity.EntityLinker

import scala.collection.JavaConversions._

object NjLinker {

  val tabSplit = "\t".r

  case class Extraction(
      val docName: String,
      val sentenceId: String,
      val arg1: String,
      val rel: String,
      val arg2: String,
      val sentenceTokens: String,
      val sentencePostags: String,
      val sentenceChunktags: String,
      var arg1Entity: Option[EntityLink],
      var arg2Entity: Option[EntityLink]) {

    def groupingKey = (arg1, rel, arg2)

    override def toString: String = {

      def formatLink(link: EntityLink): String = "%s\t%s\t%.03f\t%s".format(link.entity.name, link.entity.fbid, link.score, link.retrieveTypes.mkString(","))

      val arg1EntityString = arg1Entity match {
        case Some(link) => formatLink(link)
        case None => "No Link\tX\tX\tX"
      }

      val arg2EntityString = arg2Entity match {
        case Some(link) => formatLink(link)
        case None => "No Link\tX\tX\tX"
      }

      Seq(docName, sentenceId, arg1, rel, arg2, sentenceTokens, sentencePostags, sentenceChunktags, arg1EntityString, arg2EntityString).mkString("\t")
    }
  }


  private def error(line: String) = { System.err.println("Couldn't parse: "+line); None }

  def parseExtraction(line: String): Option[Extraction] = tabSplit.split(line) match {

    case Array(docName, sentId, str1, str2, str3, sentToks, sentPos, sentChks, _*) => {
      if (docName.startsWith("NYT_ENG")) {
        Some(Extraction(docName, sentId, arg1=str1, rel=str2, arg2=str3, sentToks, sentPos, sentChks, None, None))
      } else if (docName.startsWith("ReVerb")) {
        Some(Extraction(docName, sentId, arg1=str3, rel=str1, arg2=str2, sentToks, sentPos, sentChks, None, None))
      } else {
        error(line)
      }
    }
    case _ => error(line)
  }

  def main(args: Array[String]): Unit = {

    System.err.println("1. Parsing to internal rep...");

    val extractions = Source.fromFile(args(0)).getLines flatMap parseExtraction toIndexedSeq

    System.err.println("1. Done.");

    System.err.println("2. Running groupBy");

    val groupedExtractions = extractions groupBy(_.groupingKey)

    val numGroups = groupedExtractions.size

    System.err.println("2. Done.");

    System.err.println("3. Running linker...");

    var numLinked = 0

    val entityLinkersLocal = new ThreadLocal[Seq[EntityLinker]]() {

      override def initialValue = (1 to 4).map { num =>
        if (num == 1) new EntityLinker("/scratch/browser-freebase/")
        else new EntityLinker("/scratch%d/browser-freebase/".format(num))
      }
    }

    def randomLinker = entityLinkersLocal.get.get(scala.util.Random.nextInt(4))

    val linkedExtractions = groupedExtractions flatMap { case ((arg1, rel, arg2), group) =>

      val sentences = group map (_.sentenceTokens)

      val linker = randomLinker

      try {
        val arg1Link = linker.getBestEntity(arg1, sentences)

        val arg2Link = linker.getBestEntity(arg2, sentences)

        if (arg1Link != null) {
          group foreach { extraction => extraction.arg1Entity = Some(arg1Link) }
        }
        if (arg2Link != null) {
          group foreach { extraction => extraction.arg2Entity = Some(arg2Link) }
        }
      } catch {
        case e: Exception => e.printStackTrace
      }
      numLinked += 1
      if (numLinked % 1000 == 0) System.err.println("3. Linked so far: %d of %d".format(numLinked, numGroups))

      group
    }

    System.err.println("3. Done");

    System.err.println("4. Outputting extractions to file");

    using(new java.io.PrintWriter(args(1))) { writer => linkedExtractions foreach { extr => writer.println(extr.toString) } }

    System.err.println("4. Done");
    System.err.println("Program complete.");
  }
}