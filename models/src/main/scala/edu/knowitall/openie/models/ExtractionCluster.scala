package edu.knowitall.openie.models

import edu.knowitall.openie.models.serialize.TabSerializer
import scala.collection.mutable
import sjson.json.DefaultProtocol
import sjson.json.Format
import sjson.json.Writes
import edu.knowitall.openie.models.serialize.SpecTabFormat
import edu.knowitall.openie.models.serialize.TabFormat
import java.util.regex.Pattern
import scala.util.Try
import scala.util.Failure
import scala.util.Success

case class ExtractionCluster[+E <: Extraction](
  val arg1: ExtractionArgument,
  val rel: ExtractionRelation,
  val arg2: ExtractionArgument,
  val instances: Seq[E]) {

  def this(arg1Norm: String, relNorm: String, arg2Norm: String,
    arg1Entity: Option[FreeBaseEntity], arg2Entity: Option[FreeBaseEntity],
    arg1Types: Set[FreeBaseType], arg2Types: Set[FreeBaseType],
    instances: Seq[E]) = {
    this(ExtractionArgument(arg1Norm, arg1Entity, arg1Types),
      ExtractionRelation(relNorm),
      ExtractionArgument(arg2Norm, arg2Entity, arg2Types),
      instances)
  }

  def corpora = instances.map(_.corpus).toSet

  private val nums = "[0-9]+".r
  private val punct = "\\p{Punct}+".r
}

object ExtractionCluster {
  import Extraction._

  implicit val formatter: SpecTabFormat[ExtractionCluster[Extraction]] = TabFormat

  object TabFormat extends SpecTabFormat[ExtractionCluster[Extraction]] {
    val df = new java.text.DecimalFormat("#.###")

    override val spec: List[(String, ExtractionCluster[Extraction] => String)] = List(
      "arg1Norm" -> (_.arg1.norm),
      "relNorm" -> (_.rel.norm),
      "arg2Norm" -> (_.arg2.norm),
      "arg1Entity" -> (e => serializeEntity(e.arg1.entity)),
      "arg2Entity" -> (e => serializeEntity(e.arg2.entity)),
      "arg1Types" -> (e => serializeTypeList(e.arg1.types)),
      "arg2Types" -> (e => serializeTypeList(e.arg2.types)),
      "instances" -> (e => e.instances.iterator.map(t => implicitly[TabFormat[Extraction]].write(t)).mkString("\t")))

    override def readSeq(tokens: Seq[String]): Try[ExtractionCluster[Extraction]] = Try {

      // first take all non-instance columns
      val split = tokens.take(spec.length - 1).toIndexedSeq

      if (split.length != spec.length - 1) {
        System.err.println("Error parsing non-instance columns: " + split); (None, tokens.drop(1))
      }

      val arg1Norm = split(0)
      val relNorm = split(1)
      val arg2Norm = split(2)
      val arg1Entity = if (split(3).equals("X")) None else deserializeEntity(split(3))
      val arg2Entity = if (split(4).equals("X")) None else deserializeEntity(split(4))
      val arg1Types = if (split(5).equals("X")) Set.empty[FreeBaseType] else deserializeTypeList(split(5))
      val arg2Types = if (split(6).equals("X")) Set.empty[FreeBaseType] else deserializeTypeList(split(6))

      val formatter = implicitly[SpecTabFormat[Extraction]]
      val instances = tokens.drop(7).grouped(formatter.columns.size).map { pickledSeq =>
        formatter.readSeq(pickledSeq).get // rethrow exception
      }

      new ExtractionCluster(arg1Norm, relNorm, arg2Norm, arg1Entity, arg2Entity, arg1Types, arg2Types, instances.toSeq)
    }
  }

  object Protocol extends DefaultProtocol {
    import ExtractionProtocol._
    import ExtractionRelationProtocol._
    import ExtractionArgumentProtocol._

    implicit val ExtractionClusterFormat: Format[ExtractionCluster[Extraction]] =
      asProduct4("arg1", "rel", "arg2", "extractions")(ExtractionCluster.apply[Extraction])(ExtractionCluster.unapply(_).get)
  }

  private val commaEscapeString = Pattern.compile(Pattern.quote("|/|")) // something not likely to occur
  private val commaString = Pattern.compile(",")
  private val df = new java.text.DecimalFormat("#.###")

  private[this] def serializeEntity(opt: Option[FreeBaseEntity]): String = opt match {

    case Some(t) => {
      val escapedName = commaString.matcher(t.name).replaceAll("|/|")
      Seq(escapedName, t.fbid, df format t.score, df format t.inlinkRatio).mkString(",")
    }
    case None => "X"
  }

  // assumes that input represents an entity that is present, not empty
  private[this] def deserializeEntity(input: String): Option[FreeBaseEntity] = {

    def fail = { System.err.println("Error parsing entity: " + input); None }

    input.split(",") match {
      case Array(escapedName, fbid, scoreStr, inlinkStr, _*) => {
        val unescapedName = commaEscapeString.matcher(escapedName).replaceAll(",")
        Some(FreeBaseEntity(unescapedName, fbid, scoreStr.toDouble, inlinkStr.toDouble))
      }
      case _ => None
    }
  }

  private def serializeTypeList(types: Iterable[FreeBaseType]): String = {

    if (types.isEmpty) "X" else types.map(_.name).mkString(",")
  }

  // assumes that input represents a non-empty list
  private def deserializeTypeList(input: String): Set[FreeBaseType] = {

    val split = input.split(",").filter(str => !str.isEmpty && !str.equals("Topic")) // I have no idea how "Topic" got in as a type
    val parsed = split.flatMap(str => FreeBaseType.parse(str.toLowerCase))
    if (split.length != parsed.length) System.err.println("Warning, Type parse error for: %s".format(input))
    parsed.toSet
  }

  // we use this to combine groups that share the same entities but have
  // different norm strings (e.g. tesla and nikola tesla)
  private def entityGroupingKey(group: ExtractionCluster[Extraction]): (String, String, String) = {
    val extrKeyTuple = group.instances.head.frontendGroupingKey
    (if (group.arg1.entity.isDefined) group.arg1.entity.get.fbid else extrKeyTuple._1,
      extrKeyTuple._2,
      if (group.arg2.entity.isDefined) group.arg2.entity.get.fbid else extrKeyTuple._3)
  }

  private def frontendGroupingKey(group: ExtractionCluster[Extraction]): (String, String, String) = {
    val extrKeyTuple = group.instances.head.frontendGroupingKey
    extrKeyTuple
  }

  // pass me either all groups with the same entities or one group with an entity and the rest unlinked.
  def mergeGroups(key: (String, String, String), groups: Iterable[ExtractionCluster[Extraction]]): ExtractionCluster[Extraction] = {

    if (groups.size == 0) throw new IllegalArgumentException("can't merge zero groups")
    if (groups.size == 1) return groups.head

    val entityGroup = groups.find(g => g.arg1.entity.isDefined || g.arg2.entity.isDefined).getOrElse(groups.head)

    val allInstances = groups.flatMap(_.instances)
    val head = groups.head
    new ExtractionCluster(
      key._1,
      key._2,
      key._3,
      entityGroup.arg1.entity,
      entityGroup.arg2.entity,
      entityGroup.arg1.types,
      entityGroup.arg2.types,
      allInstances.toSeq)
  }

  /** Convert index key groups to frontend key groups, keeping entities together. */
  def indexGroupingToFrontendGrouping(groups: Iterable[ExtractionCluster[Extraction]]): Iterable[ExtractionCluster[Extraction]] = {
    // Assumes that input is grouped by "index" key. If not, behavior is undefined!

    // group groups by our frontendGroupingKey
    val entityGrouped = groups.groupBy(entityGroupingKey).map { case (key, keyGroups) => mergeGroups(key, keyGroups) }
    val candidateMergeGroups = entityGrouped.groupBy(frontendGroupingKey)
    val mergedCandidates = candidateMergeGroups.iterator.map { case (key, candidates) => mergeUnlinkedIntoLargestLinkedGroup(key, candidates.toSeq) }
    mergedCandidates.toSeq.flatten.map(group => convertKey(frontendGroupingKey(group), group))
  }

  /**
   * Given a set of groups that match according to the "frontend key", decide which ones to merge!
   * If candidates contains at most one linked entity, just merge everything. Else,
   * merge only the unlinked entities.
   */
  def mergeUnlinkedIntoLargestLinkedGroup(key: (String, String, String), candidates: Seq[ExtractionCluster[Extraction]]): Seq[ExtractionCluster[Extraction]] = {

    if (candidates.count(g => g.arg1.entity.isDefined || g.arg2.entity.isDefined) <= 1) {

      Seq(mergeGroups(key, candidates))
    } else {

      val unlinked = candidates.filter(g => g.arg1.entity.isEmpty && g.arg2.entity.isEmpty)

      val linked = candidates.filter(g => g.arg1.entity.isDefined || g.arg2.entity.isDefined)

      if (!unlinked.isEmpty) Seq(mergeGroups(key, unlinked)) ++ linked.toSeq
      else linked.toSeq
    }
  }

  // a hack to convert the norms to that of the given key
  private def convertKey(key: (String, String, String), group: ExtractionCluster[Extraction]): ExtractionCluster[Extraction] = {
    new ExtractionCluster(
      key._1,
      key._2,
      key._3,
      group.arg1.entity,
      group.arg2.entity,
      group.arg1.types,
      group.arg2.types,
      group.instances)
  }
}