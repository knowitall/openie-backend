package edu.knowitall.openie.models.serialize

import scala.Option.option2Iterable
import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.Resources
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class ReVerbTabSerializerTest extends FlatSpec {

  var inputLines: List[String] = Source.fromInputStream(Resources.reverbExtractionsUrl.openStream()).getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtraction.deserializeFromString(e))

  "extractions" should "deserialize" in {
    val extrs = getExtrsHelper
    assert(!extrs.isEmpty)
  }

  "extractions" should "round-trip through tab serialization" in {
    val extrs = getExtrsHelper
    extrs.foreach(e => assertEquals("", e, ReVerbExtraction.deserializeFromString(ReVerbExtraction.serializeToString(e)).getOrElse(fail())))
  }
}
