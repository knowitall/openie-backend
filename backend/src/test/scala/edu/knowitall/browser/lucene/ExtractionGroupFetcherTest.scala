package edu.knowitall.browser.lucene

import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractionGroupFetcherTest extends Suite {

  @Test
  def testLoadStopList: Unit = {

    val stoplist = ExtractionGroupFetcher.entityStoplist
    System.err.println("Loaded entity stop-list has size %d".format(stoplist.size))
  }
}