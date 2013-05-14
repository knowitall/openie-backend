package edu.knowitall.browser.entity

import java.io.File

object Constants {
  val mainIndexPath = "browser-freebase"
  val batchMatchPath = List(mainIndexPath, "3-context-sim", "index").mkString(File.separator)
  
  val defaultDerbyDbBasePath = "/scratch2"
  val entityLinkingDbName = "entitylinking"
  val crosswikisDbName = "crosswikis/crosswikis" // TODO: add crosswikis table to entitylinking DB.
    
  def derbyDbUrl(dbBasePath: String, dbName: String) = {
    "localhost:1527/" + List(dbBasePath, dbName).mkString(File.separator)
  }
}
