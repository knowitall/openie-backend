package edu.knowitall.browser.hadoop.scoobi.util

case class EntityInfo(val fbid: String, val types: Set[Int]) {
  override def toString = "%s,%s".format(fbid, types.mkString(","))
}

case object EntityInfo {
  def fromString(str: String) = {
    val split = str.split(",")
    val fbid = split(0)
    val types = split.drop(1).map(_.toInt).toSet
    EntityInfo(fbid, types)
  }
}