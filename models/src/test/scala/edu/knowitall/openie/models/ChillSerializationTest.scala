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
    val kryo = Chill.createBijection()
    val interval = Interval.open(4, 6)

    interval === kryo.invert(kryo(interval))
  }

  "FreeBase entities" should roundTripString in {
    val kryo = Chill.createBijection()
    val entity = FreeBaseEntity("foo", "bar", 1.0, 2.0)

    entity === kryo.invert(kryo(entity))
  }

  "FreeBase types" should roundTripString in {
    val kryo = Chill.createBijection()
    val typ = FreeBaseType.parse("/domain/type")

    typ === kryo.invert(kryo(typ))
  }

  "ChunkedToken" should roundTripString in {
    val kryo = Chill.createBijection()
    val token = new ChunkedToken(Symbol("chunk"), Symbol("postag"), "string", 2)

    token === kryo.invert(kryo(token))
  }

  "Extractions" should roundTripString in {
    val kryo = Chill.createBijection()
    val extr = TripleExtraction(0.5, "url", IndexedSeq.empty, "arg1", "rel", "arg2", Interval.open(1,3), Interval.singleton(3), Interval.closed(4,5), "source")

    extr === kryo.invert(kryo(extr))
  }

  "Set of Extractions" should roundTripString in {
    val kryo = Chill.createBijection()
    val extr1 = TripleExtraction(0.5, "url", IndexedSeq.empty, "arg1", "rel", "arg2", Interval.open(1,3), Interval.singleton(3), Interval.closed(4,5), "source1")
    val extr2 = TripleExtraction(0.5, "url", IndexedSeq.empty, "arg1", "rel", "arg2", Interval.open(2,4), Interval.singleton(4), Interval.closed(5,6), "source2")
    val set = Set(extr1, extr2)

    set === kryo.invert(kryo(set))
  }

  "Extraction" should "deserialize with Kryo from a byte array" in {
    val kryo = Chill.createBijection()
    val extr = ReVerbExtraction(IndexedSeq.empty, Interval.open(1,3), Interval.singleton(3), Interval.closed(4,5), "source")
    val pickled: Array[Byte] = Array(1, 0, 101, 100, 117, 46, 107, 110, 111, 119, 105, 116, 97, 108, 108, 46, 111, 112, 101, 110, 105, 101, 46, 109, 111, 100, 101, 108, 115, 46, 82, 101, 86, 101, 114, 98, 69, 120, 116, 114, 97, 99, 116, 105, 111, -18, 1, 1, 1, 101, 100, 117, 46, 107, 110, 111, 119, 105, 116, 97, 108, 108, 46, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110, 46, 105, 109, 109, 117, 116, 97, 98, 108, 101, 46, 73, 110, 116, 101, 114, 118, 97, -20, 1, 0, 0, 1, 2, 101, 100, 117, 46, 107, 110, 111, 119, 105, 116, 97, 108, 108, 46, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110, 46, 105, 109, 109, 117, 116, 97, 98, 108, 101, 46, 73, 110, 116, 101, 114, 118, 97, 108, 36, 67, 108, 111, 115, 101, -28, 1, 0, 0, 1, 3, 101, 100, 117, 46, 107, 110, 111, 119, 105, 116, 97, 108, 108, 46, 99, 111, 108, 108, 101, 99, 116, 105, 111, 110, 46, 105, 109, 109, 117, 116, 97, 98, 108, 101, 46, 73, 110, 116, 101, 114, 118, 97, 108, 36, 83, 105, 110, 103, 108, 101, 116, 111, 110, 73, 109, 112, -20, 1, 0, 0, 14, 1, 0, 1, 115, 111, 117, 114, 99, -27)
    println(kryo(extr).toSeq.size)
    kryo.invert(pickled) === extr
  }
}
