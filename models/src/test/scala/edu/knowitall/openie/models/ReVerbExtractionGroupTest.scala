package edu.knowitall.openie.models

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import scala.io.Source
import edu.knowitall.openie.models.serialize.Chill


@RunWith(classOf[JUnitRunner])
class ReVerbExtractionGroupTest extends FlatSpec {

  var inputLines: List[String] = Source.fromInputStream(Resources.groupsUrl.openStream(), "UTF-8").getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => { ReVerbExtractionGroup.deserializeFromString(e) })

  "The ReVerb groups" should "not be empty" in {
    val extrs = getExtrsHelper
    assert(!extrs.isEmpty)
  }

  it should "contain each element in itself" in {
    val extrSet = getExtrsHelper.toSet
    extrSet.foreach(l => assert(extrSet.contains(l)))
  }

  it should "round-trip through Java serialization" in {
    val extrs = getExtrsHelper
    extrs.foreach { e =>
      assert(e === (ReVerbExtractionGroup.deserializeFromString(ReVerbExtractionGroup.serializeToString(e)).getOrElse(fail())))
    }
  }

  it should "round-trip through Chill serialization" in {
    val kryo = Chill.createInjection(maxBufferSize = 2 << 22)
    val extrs = getExtrsHelper
    extrs.foreach { e =>
      assert(e === kryo.invert(kryo(e)).get)
    }
  }
}
