package edu.knowitall.browser.util

/**
 * A pattern for classes that can go to and from a Tab-delimited string representation.
 * Mostly useful because tabDelimitedFormatSpec provides an easy to maintain programmatic view
 * of the tab-delimited format.
 */
trait TabSerializer[T] extends StringSerializer[T] {

  private val tabSplit = "\t".r
  /**
   * A list of pairs of column names along with a function that produces the column value, given an instance of T
   */
  protected val tabDelimitedFormatSpec: List[(String, T => String)]

  /**
   * Get the Tab-Delimited header row for ReVerbExtraction
   */
  def tabDelimitedColumns: List[String] = tabDelimitedFormatSpec.map(_._1)

  override def serializeToString(extr: T): String = tabDelimitedFormatSpec.map(_._2(extr)).mkString("\t")

  /**
   * Returns (result, rest of string) e.g. by removing the part of "str" that was used.
   */

  def deserializeFromTokens(tokens: Seq[String]): Option[T]
  
  def deserializeFromString(string: String): Option[T] = deserializeFromTokens(tabSplit.split(string))

}