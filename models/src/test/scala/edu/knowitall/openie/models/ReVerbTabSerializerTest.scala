package edu.knowitall.openie.models

import scala.Option.option2Iterable
import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner
import edu.knowitall.openie.models.Resources


@RunWith(classOf[JUnitRunner])
class ReVerbTabSerializerTest extends Suite {

  var inputLines: List[String] = Source.fromInputStream(Resources.reverbExtractionsUrl.openStream()).getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtraction.deserializeFromString(e))

  @Test
  def testDeserialize = {

    val extrs = getExtrsHelper
    assert(!extrs.isEmpty)
  }


  @Test
  def testDeserializeSerialize() = {

    val extrs = getExtrsHelper
    extrs.foreach(e => assertEquals("", e, ReVerbExtraction.deserializeFromString(ReVerbExtraction.serializeToString(e)).getOrElse(fail())))
  }
}
