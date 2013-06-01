package edu.knowitall.browser.lucene

import org.junit.Assert._
import org.scalatest.junit.AssertionsForJUnit
import org.junit.Test
import org.junit.Before
import org.scalatest.Suite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.io.Source
import scala.collection.JavaConversions._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import org.apache.lucene.document.Document
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.Resources

@RunWith(classOf[JUnitRunner])
class ReVerbDocConverterTest extends Suite {

  var inputLines: List[String] = Source.fromInputStream(Resources.groupsUrl.openStream(), "UTF-8").getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtractionGroup.deserializeFromString(e))

  @Test
  def testConversion = {

    getExtrsHelper.foreach { group =>
      val doc = ReVerbDocumentConverter.toDocument(group)
      // try to deserialize the instances field and reconstruct group
      val desGroup = ReVerbDocumentConverter.fromDocument(doc)
      assertEquals(desGroup.arg1.types, group.arg1.types)
      assertEquals(desGroup.arg2.types, group.arg2.types)
      assertEquals(group, desGroup)
    }
  }

}
