package edu.knowitall.openie.models.serialize

import scala.util.control.Exception.allCatch
import com.twitter.bijection.{ Injection }
import com.twitter.chill._
import com.twitter.bijection.Bijection
import com.esotericsoftware.kryo.io.{ Input, Output }
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.openie.models._
import edu.knowitall.tool.chunk.ChunkedToken
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.KryoImplicits._
import com.twitter.bijection.ImplicitBijection
import com.esotericsoftware.kryo.{ Serializer => KSerializer }
import org.objenesis.strategy.StdInstantiatorStrategy

object Chill {
  implicit def toRicher(k: Kryo): RicherKryo = new RicherKryo(k)

  implicit val intervalBijection = Bijection.build[Interval, (Int, Int)]{
    point => (point.start, point.end)
  } { (Interval.open _).tupled }

  val chunkedTokenBijection = Bijection.build[
    ChunkedToken,
    (String, String, String, Int)
  ](ChunkedToken.unapply(_).get){ case (chunk, postag, token, offset) =>
    new ChunkedToken(chunk, postag, token, offset)
  }

  val freebaseEntityBijection = Bijection.build[
    FreeBaseEntity,
    (String, String, Double, Double)
  ](FreeBaseEntity.unapply(_).get)((FreeBaseEntity.apply _).tupled)

  val freebaseTypeBijection = Bijection.build[
    FreeBaseEntity,
    (String, String, Double, Double)
  ](FreeBaseEntity.unapply(_).get)((FreeBaseEntity.apply _).tupled)

  // we need a custom serialization to avoid Interval.  Since Interval
  // extends Seq it has some weird behavior with Kryo.  The intent is
  // that this should work, but I had some odd behavior with Intervals
  // when they were deeply nested.  I would deserialize empty intervals
  // without any error.
  def tupled(interval: Interval) = (interval.start, interval.end)
  def detupled(tuple: (Int, Int)) = (Interval.open(tuple._1, tuple._2))
  type IntervalTuple = (Int, Int)
  val reverbExtractionBijection = Bijection.build[
    ReVerbExtraction,
    (IndexedSeq[ChunkedToken], IntervalTuple, IntervalTuple, IntervalTuple, String)
  ]{
    extr => (extr.sentenceTokens, tupled(extr.arg1Interval), tupled(extr.relInterval), tupled(extr.arg2Interval), extr.sourceUrl)
  } { case (tokens, arg1Interval, relInterval, arg2Interval, source) =>
    ReVerbExtraction(tokens, detupled(arg1Interval), detupled(relInterval), detupled(arg2Interval), source)
  }
  val reverbExtractionGroupBijection = Bijection.build[
    ExtractionGroup[ReVerbExtraction],
    (ExtractionArgument, ExtractionRelation, ExtractionArgument, Set[Instance[ReVerbExtraction]])
  ](ExtractionGroup.unapply[ReVerbExtraction](_).get)((ExtractionGroup.apply[ReVerbExtraction] _).tupled)

  /**
   * Reuse the Output and Kryo, which is faster
   * register any additional serializers you need before passing in the
   * Kryo instance
   *
   * This should be available in the next release of Chill.
   */
  class KryoInjectionInstance(kryo: Kryo, output: Output) extends Injection[AnyRef, Array[Byte]] {
    private val input: Input = new Input

    def apply(obj: AnyRef): Array[Byte] = {
      output.clear
      kryo.writeClassAndObject(output, obj)
      output.toBytes
    }

    def invert(b: Array[Byte]): Option[AnyRef] = {
      input.setBuffer(b)
      allCatch.opt(kryo.readClassAndObject(input))
    }
  }


  def createInjection(): Injection[AnyRef, Array[Byte]] = {
    def myRegistrations(kryo: Kryo) = kryo
      .forClassViaBijectionDefault(intervalBijection)
      .forClassViaBijection(freebaseEntityBijection)
      .forClassViaBijection(freebaseTypeBijection)
      .forClassViaBijection(reverbExtractionBijection)
      .forClassViaBijection(reverbExtractionGroupBijection)
    val kryo = {
      val k = new KryoBase
      k.setRegistrationRequired(false)
      k.setInstantiatorStrategy(new StdInstantiatorStrategy)
      myRegistrations(k)
      KryoSerializer.registerAll(k)
      k
    }
    val output = {
      val init: Int = 1 << 10
      val max: Int = 1 << 24
      new Output(init, max)
    }
    new KryoInjectionInstance(kryo, output)
  }
}

class RicherKryo(k: Kryo) {
  def forClassViaBijectionDefault[A, B](bij: Bijection[A, B])(implicit acmf: ClassManifest[A], bcmf: ClassManifest[B]): Kryo = {
    implicit def implicitBij = bij
    val kserb = k.getSerializer(bcmf.erasure).asInstanceOf[KSerializer[B]]
    k.addDefaultSerializer(acmf.erasure, KryoSerializer.viaBijection[A, B](kserb))
    k
  }
}
