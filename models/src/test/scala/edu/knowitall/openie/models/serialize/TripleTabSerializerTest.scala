package edu.knowitall.openie.models.serialize

import scala.Option.option2Iterable
import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.Resources
import org.scalatest.junit.JUnitRunner
import edu.knowitall.openie.models.TripleExtraction
import edu.knowitall.tool.chunk.Chunker
import edu.knowitall.tool.tokenize.WhitespaceTokenizer
import edu.knowitall.collection.immutable.Interval
import edu.knowitall.openie.models.Extraction
import edu.knowitall.openie.models.ExtractionCluster

@RunWith(classOf[JUnitRunner])
class TripleTabSerializerTest extends FlatSpec {
  import Extraction._
  import ExtractionCluster._

  "extractions" should "round-trip through tab serialization" in {
    val tokens = Chunker.tokensFrom("B-NP B-VP B-NP I-NP".split(" "), "NN VB DT NN".split(" "), WhitespaceTokenizer.tokenize("This is a test"))
    val extr = new TripleExtraction(0.0, "corpus", tokens, "this", "is", "a", Interval.open(0, 1), Interval.open(1, 2), Interval.open(2, 3), "url")
    implicitly[TabFormat[Extraction]].read(implicitly[TabFormat[Extraction]].write(extr)).get === extr
  }

  "extraction clusters" should "round-trip through tab serialization" in {
    val tokens = Chunker.tokensFrom("B-NP B-VP B-NP I-NP".split(" "), "NN VB DT NN".split(" "), WhitespaceTokenizer.tokenize("This is a test"))
    val extr = new TripleExtraction(0.0, "corpus", tokens, "this", "is", "a", Interval.open(0, 1), Interval.open(1, 2), Interval.open(2, 3), "url")
    val cluster = new ExtractionCluster("foo", "bar", "baz", None, None, Set.empty, Set.empty, Seq(extr))
    implicitly[TabFormat[ExtractionCluster[Extraction]]].read(implicitly[TabFormat[ExtractionCluster[Extraction]]].write(cluster)).get === cluster
  }
}
