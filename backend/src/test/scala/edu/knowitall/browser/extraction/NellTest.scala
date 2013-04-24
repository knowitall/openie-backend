package edu.knowitall.openie.models

import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class NellExtractionTest extends Suite {


  @Test
  def testFbToNell() = {

    val magazine = FreeBaseType.parse("/book/magazine").get

    val nellType = NellType.fbToNellType(magazine)

    if (!nellType.equals(NellType("magazine", "magazine"))) fail
  }
}
