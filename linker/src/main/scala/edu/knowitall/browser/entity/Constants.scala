package edu.knowitall.browser.entity

import java.io.File

object Constants {
  // Parent directory with lots of supporting data.
  def mainIndexPath(basePath: File) = new File(basePath, "browser-freebase")
  
  // Supporting data for entity linking.
  def fbidToTitleInlinksFile(basePath: File) = {
    new File(mainIndexPath(basePath), "fbid_to_line.sorted")
  }
  def titleMapFile(basePath: File) = {
    new File(mainIndexPath(basePath), "titleMap.sorted")
  }
  def cachedMapFile(basePath: File) = {
    new File(mainIndexPath(basePath), "cached.sorted")
  }
  
  // Supporting data for entity linking context matching.
  def batchMatchPath(basePath: File) = {
    new File(new File(mainIndexPath(basePath), "3-context-sim"), "index")
  }
  
  // Supporting data for entity typing.
  def typeLookupIndexPath(basePath: File) = {
    new File(mainIndexPath(basePath), "type-lookup-index")
  }
  def freeBaseTypeEnumFile(basePath: File) = {
    new File(mainIndexPath(basePath), "fbTypeEnum.txt")
  }
  
  // Derby database paths.
  val defaultDerbyDbBasePath = new File("/scratch2")
  def entityLinkingDbPath(basePath: File) = {
    new File("/scratch2", "entitylinking")
  }
  // TODO: add crosswikis table to entitylinking DB.
  def crosswikisDbPath(basePath: File) = {
    new File(new File(basePath, "crosswikis"), "crosswikis")
  }
  def derbyDbUrl(dbPath: File) = {
    "localhost:1527/%s/".format(dbPath.getCanonicalPath())
  }
}
