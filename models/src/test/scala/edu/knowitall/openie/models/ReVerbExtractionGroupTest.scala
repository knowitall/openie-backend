package edu.knowitall.openie.models

import scala.Option.option2Iterable
import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ReVerbExtractionGroupTest extends Suite {

  var inputLines: List[String] = Source.fromInputStream(Resources.groupsUrl.openStream(), "UTF-8").getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => { ReVerbExtractionGroup.deserializeFromString(e) })

  @Test
  def testDeserialize(): Unit = {

    val extrs = getExtrsHelper
    assert(!extrs.isEmpty)
  }

  @Test
  def testEquality(): Unit = {

    val extrSet = getExtrsHelper.toSet
    extrSet.foreach(l => assert(extrSet.contains(l)))
  }

  @Test
  def testDeserializeSerialize() = {

    val extrs = getExtrsHelper
    extrs.foreach{ e =>
      assertEquals("", e, ReVerbExtractionGroup.deserializeFromString(ReVerbExtractionGroup.serializeToString(e)).getOrElse(fail()))
    }
  }
}
