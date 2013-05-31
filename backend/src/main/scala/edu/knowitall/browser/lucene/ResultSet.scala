package edu.knowitall.browser.lucene

import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.ReVerbExtractionGroup

sealed abstract class ResultSet() {
  def results: List[ExtractionGroup[ReVerbExtraction]]

  lazy val numInstances = results.map(_.instances.size).sum

  def numGroups = results.size

  def combineWith(other: ResultSet): ResultSet
}

case class Success(override val results: List[ExtractionGroup[ReVerbExtraction]]) extends ResultSet {

  override def combineWith(other:ResultSet): ResultSet = other match {
    case Success(otherResults) => Success(results ++ otherResults)
    case _ => other.combineWith(this)
  }
}
case object Success { val empty = Success(List.empty) }

case class Limited(override val results: List[ExtractionGroup[ReVerbExtraction]], val totalGroups: Int) extends ResultSet {

  override def combineWith(other: ResultSet): ResultSet = other match {
    case Success(otherResults) => Limited(results ++ otherResults, totalGroups + other.numGroups)
    case Limited(otherResults, otherTotalGroups) => Limited(results ++ otherResults, totalGroups + otherTotalGroups)
    case _ => other.combineWith(this)
  }
}
case object Limited { val empty = Limited(List.empty, 0) }

case class Timeout(override val results: List[ExtractionGroup[ReVerbExtraction]], val totalGroups: Int) extends ResultSet {

  override def combineWith(other: ResultSet): ResultSet = other match {
    case Success(otherResults) => Timeout(results ++ otherResults, totalGroups + other.numGroups)
    case Limited(otherResults, otherTotalGroups) => Timeout(results ++ otherResults, totalGroups + otherTotalGroups)
    case Timeout(otherResults, otherTotalGroups) => Timeout(results ++ otherResults, totalGroups + otherTotalGroups)
  }
}
case object Timeout { val empty = Timeout(List.empty, 0) }
