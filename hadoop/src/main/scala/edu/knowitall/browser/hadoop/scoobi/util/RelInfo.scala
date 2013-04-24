package edu.knowitall.browser.hadoop.scoobi.util

case class RelInfo(val string: String, val weight: Double, val entities: Set[EntityInfo]) {
  val strings = Seq(string, "%.02f".format(weight)) ++ entities.map(_.toString)
  override def toString = strings.map(_.replaceAll(":", "_COLON_")).mkString(":")
}
case object RelInfo {
  def fromString(str: String) = {
    try {
      val split = str.split(":").map(_.replaceAll("_COLON_", ":"))
      val string = split(0)
      val weight = split(1).toDouble
      val entities = split.drop(2) map EntityInfo.fromString
      Some(RelInfo(string, weight, entities.toSet))
    } catch {
      case e: Exception => {
        e.printStackTrace
        System.err.println("RelInfo parse error: %s, continuing...".format(str))
        None
      }
    }
  }
}