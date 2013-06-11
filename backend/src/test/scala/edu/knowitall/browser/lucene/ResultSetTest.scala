package edu.knowitall.browser.lucene

import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ResultSetTest extends Suite {


  @Test
  def testCombine() = {

    assert((Success.empty combineWith Limited.empty).isInstanceOf[Limited])
    assert((Limited.empty combineWith Success.empty).isInstanceOf[Limited])
    assert((Success.empty combineWith Timeout.empty).isInstanceOf[Timeout])
    assert((Limited.empty combineWith Timeout.empty).isInstanceOf[Timeout])
  }
}
