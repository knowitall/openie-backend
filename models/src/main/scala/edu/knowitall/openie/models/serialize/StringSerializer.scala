package edu.knowitall.openie.models.serialize

/**
 * A trait for classes that can be serialized to strings, and deserialized from strings. 
 * In general, it should be true that deserializeFromString(serializeToString(t)).equals(t)
 * for any instance of T
 */
trait StringSerializer[T] {

  /** Convert t to a string representation */
  def serializeToString(t: T): String
  
  /** Parse str into an instance of T, None if there is an error in parsing */
  def deserializeFromString(str: String): Option[T]
}