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
    new ChunkedToken(Symbol(chunk), Symbol(postag), token, offset)
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
  val extractionTripleBijection = Bijection.build[
    TripleExtraction,
    (Double, String, Seq[ChunkedToken], String, String, String, IntervalTuple, IntervalTuple, IntervalTuple, String)
  ]{
    extr => (extr.confidence, extr.corpus, extr.sentenceTokens, extr.arg1Text, extr.relText, extr.arg2Text, tupled(extr.arg1Interval), tupled(extr.relInterval), tupled(extr.arg2Interval), extr.source)
  } { case (confidence, corpus, tokens, arg1Text, relText, arg2Text, arg1Interval, relInterval, arg2Interval, source) =>
    TripleExtraction(confidence, corpus, tokens, arg1Text, relText, arg2Text, detupled(arg1Interval), detupled(relInterval), detupled(arg2Interval), source)
  }
  def extractionClusterBijection[T <: Extraction] = Bijection.build[
    ExtractionCluster[T],
    (ExtractionArgument, ExtractionRelation, ExtractionArgument, Seq[T])
  ](ExtractionCluster.unapply[T](_).get)((ExtractionCluster.apply[T] _).tupled)

  /**
   * Reuse the Output and Kryo, which is faster
   * register any additional serializers you need before passing in the
   * Kryo instance
   *
   * This should be available in the next release of Chill.
   */
  class KryoBijectionInstance(kryo: Kryo, output: Output) extends Bijection[AnyRef, Array[Byte]] {
    private val input: Input = new Input

    def apply(obj: AnyRef): Array[Byte] = {
      output.clear
      kryo.writeClassAndObject(output, obj)
      output.toBytes
    }

    override def invert(b: Array[Byte]): AnyRef = {
      input.setBuffer(b)
      kryo.readClassAndObject(input)
    }
  }


  def createBijection(initBufferSize: Int = 1 << 10, maxBufferSize: Int = 1 << 24): Bijection[AnyRef, Array[Byte]] = {
    def myRegistrations(kryo: Kryo) = kryo
      .forClassViaBijectionDefault2(intervalBijection)
      .forClassViaBijection(freebaseEntityBijection)
      .forClassViaBijection(freebaseTypeBijection)
      .forClassViaBijection(extractionTripleBijection)
      .forClassViaBijection(extractionClusterBijection[Extraction])
    val kryo = {
      val k = new KryoBase
      k.setRegistrationRequired(false)
      k.setInstantiatorStrategy(new StdInstantiatorStrategy)
      myRegistrations(k)
      KryoSerializer.registerAll(k)
      k
    }
    val output = {
      val init: Int = initBufferSize
      val max: Int = maxBufferSize
      new Output(init, max)
    }
    new KryoBijectionInstance(kryo, output)
  }
}

class RicherKryo(k: Kryo) {
  def forClassViaBijection[A, B](bij: Bijection[A, B])(implicit acmf: ClassManifest[A], bcmf: ClassManifest[B]): Kryo = {
    implicit def implicitBij = bij
    val kserb = k.getSerializer(bcmf.erasure).asInstanceOf[KSerializer[B]]
    k.register(acmf.erasure, KryoSerializer.viaBijection[A, B](kserb))
    k
  }

  def forClassViaBijectionDefault2[A, B](bij: Bijection[A, B])(implicit acmf: ClassManifest[A], bcmf: ClassManifest[B]): Kryo = {
    implicit def implicitBij = bij
    val kserb = k.getSerializer(bcmf.erasure).asInstanceOf[KSerializer[B]]
    k.addDefaultSerializer(acmf.erasure, KryoSerializer.viaBijection[A, B](kserb))
    k
  }
}
