package edu.knowitall.browser.lucene

import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.NumericField
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.FreeBaseEntity
import edu.knowitall.openie.models.FreeBaseType
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtraction

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

import scala.collection.JavaConversions._

object ReVerbDocumentConverter {

  def toDocument(group: ExtractionGroup[ReVerbExtraction]): Document = {

    val doc = new Document()

    ///
    /// INDEXED FIELDS
    ///

    // add arg/rel norms
    doc.add(new Field("arg1Norm", group.arg1.norm, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("relNorm", group.rel.norm, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("arg2Norm", group.arg2.norm, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));

    // add searchable field for source corpora
    doc.add(new Field("corpora", group.corpora.mkString(" "), Field.Store.NO, Field.Index.ANALYZED_NO_NORMS))

    // if arg1 has a linked entity, add fields for its name and ID, and types
    group.arg1.entity match {
      case Some(entity) => {
        doc.add(new Field("arg1EntityName", entity.name, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
        doc.add(new Field("arg1EntityId", entity.fbid, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field("arg1EntityScore", entity.score.toString, Field.Store.YES, Field.Index.NO))
        doc.add(new Field("arg1EntityInlinks", entity.inlinkRatio.toString, Field.Store.YES, Field.Index.NO))
      }
      case None =>
    }
    if (!group.arg1.types.isEmpty) {
      doc.add(new Field("arg1Types", group.arg1.types.toSeq.map(_.typ).mkString(" "), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("arg1TypeDomains", group.arg1.types.toSeq.map(_.domain.toLowerCase).mkString(" "), Field.Store.YES, Field.Index.NO))
    }

    // if arg2 has a linked entity, add fields for its name and ID, and types
    group.arg2.entity match {
      case Some(entity) => {
        doc.add(new Field("arg2EntityName", entity.name, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
        doc.add(new Field("arg2EntityId", entity.fbid, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
        doc.add(new Field("arg2EntityScore", entity.score.toString, Field.Store.YES, Field.Index.NO))
        doc.add(new Field("arg2EntityInlinks", entity.inlinkRatio.toString, Field.Store.YES, Field.Index.NO))
      }
      case None =>
    }

    if (!group.arg2.types.isEmpty) {
      doc.add(new Field("arg2Types", group.arg2.types.toSeq.map(_.typ.toLowerCase).mkString(" "), Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      doc.add(new Field("arg2TypeDomains", group.arg2.types.toSeq.map(_.domain.toLowerCase).mkString(" "), Field.Store.YES, Field.Index.NO))
    }

    // if relation has a link, add a field for it
    group.rel.link match {
      case Some(link) => {
        doc.add(new Field("relLink", link, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS))
      }
      case None =>
    }

    ///
    /// NON-INDEXED FIELDS
    ///

    // add a Numeric field for the number of instances in this group
    val sizeField = new NumericField("size", Field.Store.YES, true).setIntValue(group.instances.size)
    doc.add(sizeField)

    // finally, serialize all of group.instances to a list and stuff it in one big binary field
    val byteOutput = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(byteOutput)
    oos.writeObject(group.instances.toSeq.toStream)
    val instancesBytes = byteOutput.toByteArray()
    byteOutput.close()
    oos.close()

    doc.add(new Field("instances", instancesBytes))

    doc
  }

  private def parseTypeList(arg1TypeDomains: String, arg1Types: String): Set[FreeBaseType] = {
    val domains = arg1TypeDomains.split(" ").filter(!_.isEmpty)
    val types = arg1Types.split(" ").filter(!_.isEmpty)
    val zipped = domains.zip(types)
    if (domains.length != types.length) System.err.println("Warning, FreeBaseType parse error for: %s\n%s".format(domains.mkString(","), types.mkString(",")))
    zipped.map { case (domain, types) =>
      FreeBaseType(domain, types)
    } toSet
  }

  def fromDocument(doc: Document): ExtractionGroup[ReVerbExtraction] = {

    // get a map of field name to field. We assume fields are unique by name.
    val fields = doc.getFields.groupBy(_.name()).map(pair => (pair._1, pair._2.head)).toMap

    val arg1Entity = (fields.get("arg1EntityName"), fields.get("arg1EntityId"), fields.get("arg1EntityScore"),fields.get("arg1EntityInlinks")) match {
      case (Some(nameField), Some(idField), Some(scoreField), Some(inlinkField)) => {
        val name = nameField.stringValue
        val id = idField.stringValue
        val score = scoreField.stringValue.toDouble
        val inlinks = inlinkField.stringValue.toDouble
        Some(new FreeBaseEntity(name, id, score, inlinks))
      }
      case _ => None
    }

    val arg1Types = (fields.get("arg1TypeDomains"), fields.get("arg1Types")) match {
      case (Some(domains), Some(types)) => {
        parseTypeList(domains.stringValue, types.stringValue)
      }
      case _ => Set.empty[FreeBaseType]
    }

    val arg2Entity = (fields.get("arg2EntityName"), fields.get("arg2EntityId"), fields.get("arg2EntityScore"),fields.get("arg2EntityInlinks")) match {
      case (Some(nameField), Some(idField), Some(scoreField), Some(inlinkField)) => {
        val name = nameField.stringValue
        val id = idField.stringValue
        val score = scoreField.stringValue.toDouble
        val inlinks = inlinkField.stringValue.toDouble
        Some(new FreeBaseEntity(name, id, score, inlinks))
      }
      case _ => None
    }

    val arg2Types = (fields.get("arg2TypeDomains"), fields.get("arg2Types")) match {
      case (Some(domains), Some(types)) => {
        parseTypeList(domains.stringValue, types.stringValue)
      }
      case _ => Set.empty[FreeBaseType]
    }

    val relLink = fields.get("relLink") match {
      case Some(link) => Some(link.stringValue)
      case _ => None
    }

    val byteInput = new ByteArrayInputStream(fields("instances").getBinaryValue())
    val objectInput = new ObjectInputStream(byteInput)
    val instances = objectInput.readObject().asInstanceOf[Stream[Instance[ReVerbExtraction]]]

    val desGroup = new ExtractionGroup(
      fields("arg1Norm").stringValue(),
      fields("relNorm").stringValue(),
      fields("arg2Norm").stringValue(),
      arg1Entity,
      arg2Entity,
      arg1Types,
      arg2Types,
      relLink,
      instances.toSet)

    desGroup
  }
}
