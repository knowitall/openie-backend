package edu.knowitall.browser.lucene

trait GroupFetcher {

  def getGroups(arg1: Option[String] = None, rel: Option[String] = None, arg2: Option[String] = None, stem: Boolean = true): ResultSet = {
    val querySpec = QuerySpec(arg1, rel, arg2, arg1, arg2, None, None, None, stem)
    getGroups(querySpec)
  }
  
  def getGroups(querySpec: QuerySpec): ResultSet
}