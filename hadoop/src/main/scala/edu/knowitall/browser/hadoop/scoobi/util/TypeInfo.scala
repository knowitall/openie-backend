package edu.knowitall.browser.hadoop.scoobi.util

import edu.knowitall.common.Resource.using
import scala.io.Source
import edu.knowitall.browser.hadoop.scoobi.UnlinkableEntityTyper
import edu.knowitall.browser.hadoop.scoobi.UnlinkableEntityTyper.tabSplit

case class TypeInfo(val typeString: String, val enum: Int, val instances: Int)

object TypeInfoUtils {

  val typeEnumFile = "/fbTypeEnum.txt"
  val typeBlacklistFile = "/type_blacklist.txt"

  def getResourceSource(resourceName: String): Source = Source.fromInputStream(UnlinkableEntityTyper.getClass.getResource(resourceName).openStream)
  def getEnumSource = getResourceSource(typeEnumFile)
  // type, num, count
  def parseEnumLine(line: String): TypeInfo = { val split = tabSplit.split(line); TypeInfo(split(1), split(0).toInt, split(2).toInt) }

  lazy val typeStringMap = using(getEnumSource) { _.getLines map parseEnumLine map (ti => (ti.typeString, ti)) toMap }
  lazy val typeEnumMap = using(getEnumSource) { _.getLines map parseEnumLine map (ti => (ti.enum, ti)) toMap }
  lazy val typeBlacklist = using(getResourceSource(typeBlacklistFile)) { _.getLines toSet }

  def typeFilter(typeString: String): Boolean = {
    !typeString.startsWith("/base/") && !typeBlacklist.contains(typeString)
  }
}

case class TypePrediction(
  val argString: String,
  val predictedTypes: Seq[(TypeInfo, Int)],
  val bestRels: Seq[(String, Double)],
  val totalEntityWeight: Double,
  val topSimilarFbids: Seq[String]) {

  override def toString: String = {

    def typeShareToString(tPair: (TypeInfo, Int)): String = Seq(tPair._1.typeString, tPair._2.toString).map(_.replaceAll("@", "_ATSYM_")).mkString("@")
    def relPairToString(rPair: (String, Double)): String = Seq(rPair._1, "%.02f".format(rPair._2)).map(_.replaceAll(":", "_COLON_")).mkString(":")

    val typesString = predictedTypes map typeShareToString mkString (", ")
    val bestRelsString = bestRels map relPairToString mkString (", ")
    val weightString = "%.02f".format(totalEntityWeight)
    val fbidsString = topSimilarFbids.mkString(",")
    Seq(argString, typesString, bestRelsString, weightString, fbidsString).map(_.replaceAll("\t", "_TAB_")).mkString("\t")
  }
}

case object TypePrediction {
  def fromString(str: String): Option[TypePrediction] = {

    def fail(msg: String) = { System.err.println("TypePrediction Parse Error: %s".format(msg)); None }

    def typeShareFromString(str: String): Option[(TypeInfo, Int)] = {
      str.split("@").map(_.replaceAll("_ATSYM_", "@")) match {
        case Array(typeString, shareScoreString, _*) => TypeInfoUtils.typeStringMap.get(typeString) map { typeInfo => (typeInfo, shareScoreString.toInt) }
        case _ => None
      }
    }
    def relPairFromString(str: String): Option[(String, Double)] = {
      str.split(":").map(_.replaceAll("_COLON_", ":")) match {
        case Array(relString, weightString, _*) => Some((relString, weightString.toDouble))
        case _ => None
      }
    }

    tabSplit.split(str).map(_.replaceAll("_TAB_", "\t")) match {
      case Array(argString, typesString, bestRelsString, weightString, fbidsString, _*) => {
        val predictedTypes = typesString.split(", ") flatMap typeShareFromString
        def bestRels = bestRelsString.split(", ") flatMap relPairFromString
        def topSimilarFbids = fbidsString.split(",")
        def totalEntityWeight = weightString.toDouble
        def typePred = TypePrediction(argString, predictedTypes, bestRels, totalEntityWeight, topSimilarFbids)
        if (predictedTypes.isEmpty) fail(str) else Some(typePred)
      }
      case _ => fail("(bad split) %s".format(str))
    }
  }
}