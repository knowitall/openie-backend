package edu.knowitall.openie.models

import edu.knowitall.openie.models.serialize.TabSerializer
import scala.collection.mutable
import sjson.json.DefaultProtocol
import sjson.json.Format
import sjson.json.Writes

@SerialVersionUID(4831174870091219621L)
case class Instance[+E <: Extraction](val extraction: E, val corpus: String, val confidence: Double)

object InstanceProtocol extends DefaultProtocol {
  import ExtractionProtocol._

  implicit val InstanceFormat: Format[Instance[Extraction]] =
    asProduct3("extraction", "corpus", "confidence")(Instance.apply[Extraction])(Instance.unapply(_).get)

  implicit val ReVerbInstanceFormat: Format[Instance[ReVerbExtraction]] =
    asProduct3("extraction", "corpus", "confidence")(Instance.apply[ReVerbExtraction])(Instance.unapply(_).get)
}

sealed abstract class ExtractionPart() {
  def norm: String
}
case class ExtractionArgument(
    override val norm: String,
    val entity: Option[FreeBaseEntity],
    val types: Set[FreeBaseType]) extends ExtractionPart {
  def hasEntity = entity.isDefined
}

case class ExtractionRelation(
    override val norm: String,
    val link: Option[String] = None) extends ExtractionPart {
  def hasLink = link.isDefined
}


@SerialVersionUID(6000)
case class ExtractionGroup[E <: Extraction](
  val arg1: ExtractionArgument,
  val rel: ExtractionRelation,
  val arg2: ExtractionArgument,
  val instances: Set[Instance[E]]) {

  def this(arg1Norm: String, relNorm: String, arg2Norm: String,
      arg1Entity: Option[FreeBaseEntity], arg2Entity: Option[FreeBaseEntity],
      arg1Types: Set[FreeBaseType], arg2Types: Set[FreeBaseType],
      instances: Set[Instance[E]]) = {
    this(ExtractionArgument(arg1Norm, arg1Entity, arg1Types),
      ExtractionRelation(relNorm, None),
      ExtractionArgument(arg2Norm, arg2Entity, arg2Types),
      instances)
  }

  def this(arg1Norm: String, relNorm: String, arg2Norm: String,
      arg1Entity: Option[FreeBaseEntity], arg2Entity: Option[FreeBaseEntity],
      arg1Types: Set[FreeBaseType], arg2Types: Set[FreeBaseType], relLink: Option[String],
      instances: Set[Instance[E]]) = {
    this(ExtractionArgument(arg1Norm, arg1Entity, arg1Types),
      ExtractionRelation(relNorm, relLink),
      ExtractionArgument(arg2Norm, arg2Entity, arg2Types),
      instances)
  }


  def corpora = instances.map(_.corpus).toSet

  def setArg1(newArg1: ExtractionArgument): ExtractionGroup[E] = this.copy(arg1 = newArg1)
  def setArg2(newArg2: ExtractionArgument): ExtractionGroup[E] = this.copy(arg2 = newArg2)

  def removeArg1Entity = setArg1(ExtractionArgument(arg1.norm, None, Set.empty))
  def removeArg2Entity = setArg2(ExtractionArgument(arg2.norm, None, Set.empty))

  /**
    * Use the indexing key to try to fracture this group.
    * Assumes that each resulting group still has the same entity and type links
    */
  def reNormalize: Set[ExtractionGroup[E]] = {
    val identitySet = Set(this)
    if (instances.size == 1) return identitySet
    val newGroupsMap = instances.groupBy(_.extraction.indexGroupingKey)
    val newGroups = newGroupsMap.map {
      case (normKey, subInstances) =>
        new ExtractionGroup[E](ExtractionArgument(normKey._1, arg1.entity, arg1.types),
          ExtractionRelation(normKey._2, rel.link),
          ExtractionArgument(normKey._3, arg2.entity, arg2.types),
          subInstances)
    } toSet

    if (Set(this).equals(newGroups)) {
      identitySet
    } else {
      newGroups
    }
  }

  private val nums = "[0-9]+".r
  private val punct = "\\p{Punct}+".r
}

object ExtractionGroupProtocol extends DefaultProtocol {
  import InstanceProtocol._
  import ExtractionRelationProtocol._
  import ExtractionArgumentProtocol._

  implicit val ExtractionGroupFormat: Format[ExtractionGroup[ReVerbExtraction]] =
    asProduct4("arg1", "rel", "arg2", "instances")(ExtractionGroup.apply[ReVerbExtraction])(ExtractionGroup.unapply(_).get)
}

object ExtractionRelationProtocol extends DefaultProtocol {
  implicit val ExtractionRelationFormat: Format[ExtractionRelation] =
    asProduct2("lemma", "link")(ExtractionRelation.apply)(ExtractionRelation.unapply(_).get)
}

object ExtractionArgumentProtocol extends DefaultProtocol {
  import FreeBaseTypeProtocol._
  import FreeBaseEntityProtocol._

  implicit val ExtractionArgumentFormat: Format[ExtractionArgument] =
    asProduct3("lemma", "entity", "types")(ExtractionArgument.apply)(ExtractionArgument.unapply(_).get)
}
