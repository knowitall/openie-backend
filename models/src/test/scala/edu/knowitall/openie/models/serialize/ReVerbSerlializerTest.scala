package edu.knowitall.openie.models.serialize

import scala.Option.option2Iterable
import scala.io.Source
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.Resources
import Chill._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReVerbSerializerTest extends FlatSpec {
  var inputLines: List[String] = Source.fromInputStream(Resources.reverbExtractionsUrl.openStream(), "UTF-8").getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtraction.deserializeFromString(e))

  val kryo = Chill.createBijection()

  "extractions" should "deserialize" in {
    val extrs = getExtrsHelper
    extrs.foreach { extr =>

      val extrBytes = kryo.apply(extr)
      val deserializedExtr = kryo.inverse(extrBytes)

      extr === deserializedExtr
    }
  }
}
