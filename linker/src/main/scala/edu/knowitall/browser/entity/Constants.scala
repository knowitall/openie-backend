package edu.knowitall.browser.entity

import java.io.File

object Constants {
  val mainIndexPath = "browser-freebase"
  val batchMatchPath = List(mainIndexPath, "3-context-sim", "index").mkString(File.separator)
  
  val defaultDerbyDbPath = "/scratch2"
  def derbyDbUrl(dbPath: String) = {
    List("localhost:1527/", dbPath, "entitylinking").mkString(File.separator)
  }
}
