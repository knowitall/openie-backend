package edu.knowitall.openie.models

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import scala.io.Source
import edu.knowitall.openie.models.serialize.Chill
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.tool.chunk.ChunkedToken
import com.esotericsoftware.kryo.Kryo


@RunWith(classOf[JUnitRunner])
class ChillSerializationTest extends FlatSpec {
  val roundTripString = "round trip through Kryo"

  "intervals" should roundTripString in {
    val kryo = Chill.createInjection()
    val interval = Interval.open(4, 6)

    interval === kryo.invert(kryo(interval))
  }

  "FreeBase entities" should roundTripString in {
    val kryo = Chill.createInjection()
    val entity = FreeBaseEntity("foo", "bar", 1.0, 2.0)

    entity === kryo.invert(kryo(entity))
  }

  "FreeBase types" should roundTripString in {
    val kryo = Chill.createInjection()
    val typ = FreeBaseType.parse("/domain/type")

    typ === kryo.invert(kryo(typ))
  }

  "ChunkedToken" should roundTripString in {
    val kryo = Chill.createInjection()
    val token = new ChunkedToken("chunk", "postag", "string", 2)

    token === kryo.invert(kryo(token))
  }

  "ReVerbExtractions" should roundTripString in {
    val kryo = Chill.createInjection()
    val extr = ReVerbExtraction(IndexedSeq.empty, Interval.open(1,3), Interval.singleton(3), Interval.closed(4,5), "source")

    extr === kryo.invert(kryo(extr))
  }

  "Set of ReVerbExtractions" should roundTripString in {
    val kryo = Chill.createInjection()
    val extr1 = ReVerbExtraction(IndexedSeq.empty, Interval.open(1,3), Interval.singleton(3), Interval.closed(4,5), "source1")
    val extr2 = ReVerbExtraction(IndexedSeq.empty, Interval.open(2,4), Interval.singleton(4), Interval.closed(5,6), "source2")
    val set = Set(extr1, extr2)

    set === kryo.invert(kryo(set))
  }

  "ReVerbExtraction" should "deserialize with Kryo from a byte array" in {
    val kryo = Chill.createInjection()
    val extr = ReVerbExtraction(IndexedSeq.empty, Interval.open(1,3), Interval.singleton(3), Interval.closed(4,5), "source")
    val pickled: Array[Byte] = Array(12, 1, 14, 2, 0, 41, 3, 0, 0, 0, 1, 0, 0, 0, 3, 41, 4, 0, 0, 0, 3, 0, 0, 0, 4, 41, 5, 0, 0, 0, 4, 0, 0, 0, 6, 3, 6, 115, 111, 117, 114, 99, -27)
    kryo.invert(pickled) === extr
  }
}
