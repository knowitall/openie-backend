package edu.knowitall.openie.models.serialize

import scala.util.Try

trait TabWriter[T] {
  def write(item: T): String
}

trait TabReader[T] {
  def read(string: String): Try[T]
}

object TabFormat {
  val tabRegex = "\t".r
}

trait TabFormat[T] extends TabWriter[T] with TabReader[T] {
}

trait SpecTabFormat[T] extends TabFormat[T] {
   /**
   * A list of pairs of column names along with a function that produces the column value, given an instance of T
   */
  def spec: List[(String, T => String)]
  def columns: List[String] = spec.map(_._1)
  def functions: List[T => String] = spec.map(_._2)

  override def write(item: T) = {
    val strings = functions map (f => f(item))
    strings.mkString("\t")
  }

  override def read(pickled: String) = {
    readSeq(TabFormat.tabRegex split pickled)
  }

  def readSeq(seq: Seq[String]): Try[T]

  def readToMap(pickled: String) = {
    val split = TabFormat.tabRegex.split(pickled)

    require(split.size == spec.size, "Split size != spec size: " + pickled)

    (columns.iterator zip split.iterator).toMap
  }
}
