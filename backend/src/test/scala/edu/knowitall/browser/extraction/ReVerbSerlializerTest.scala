package edu.knowitall.browser.extraction

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.Option.option2Iterable
import scala.io.Source
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReVerbSerializerTest extends Suite {

  var inputLines: List[String] = Source.fromInputStream(this.getClass.getResource("/test-groups-5000.txt").openStream(), "UTF-8").getLines.toList

  private def getExtrsHelper = inputLines.flatMap(e => ReVerbExtraction.deserializeFromString(e))

  @Test
  def testSerialization = {

    val extrs = getExtrsHelper
    extrs.foreach { extr =>

      val extrBytes = extrToBytes(extr)
      val deserializedExtr = extrFromBytes(extrBytes)

      assertEquals(extr, deserializedExtr)
    }
  }

  private def extrFromBytes(bytes: Array[Byte]): ReVerbExtraction = {
    val byteInput = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(byteInput)
    val obj = ois.readObject
    val deserializedExtr = obj.asInstanceOf[ReVerbExtraction]
    byteInput.close
    ois.close
    return deserializedExtr
  }

  private def extrToBytes(extr: ReVerbExtraction): Array[Byte] = {
    val byteOutput = new ByteArrayOutputStream()
    // serialize to bytes
    val oos = new ObjectOutputStream(byteOutput)
    oos.writeObject(extr)
    val bytes = byteOutput.toByteArray
    oos.close
    byteOutput.close
    return bytes
  }
}
