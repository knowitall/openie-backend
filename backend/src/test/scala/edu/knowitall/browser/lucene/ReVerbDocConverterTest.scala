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

import edu.knowitall.browser.extraction.ReVerbExtractionGroup
import edu.knowitall.browser.extraction.ExtractionGroup
import edu.knowitall.browser.extraction.ReVerbExtraction
import edu.knowitall.browser.extraction.FreeBaseEntity
import edu.knowitall.browser.extraction.Instance

@RunWith(classOf[JUnitRunner])
class ReVerbDocConverterTest extends Suite {

  var inputLines: List[String] = Source.fromInputStream(this.getClass.getResource("/test-groups-5000.txt").openStream(), "UTF-8").getLines.toList

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
