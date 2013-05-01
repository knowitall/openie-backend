package edu.knowitall.openie.models

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class NellExtractionTest extends FlatSpec {
  "the FreeBase type" should "convert to a Nell type" in {
    val magazine = FreeBaseType.parse("/book/magazine").get

    val nellType = NellType.fbToNellType(magazine)

    nellType === NellType("magazine", "magazine")
  }
}
