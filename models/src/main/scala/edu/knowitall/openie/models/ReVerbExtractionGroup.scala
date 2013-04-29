package edu.knowitall.openie.models

import java.util.regex.Pattern

import edu.knowitall.openie.models.serialize.TabSerializer

import scala.collection.mutable



object ReVerbExtractionGroup extends TabSerializer[ExtractionGroup[ReVerbExtraction]] {

  type REG = ExtractionGroup[ReVerbExtraction]

  val df = new java.text.DecimalFormat("#.###")

  val tabDelimitedFormatSpec: List[(String, REG => String)] = List(
    "arg1Norm" -> (_.arg1.norm),
    "relNorm" -> (_.rel.norm),
    "arg2Norm" -> (_.arg2.norm),
    "arg1Entity" -> (e => serializeEntity(e.arg1.entity)),
    "arg2Entity" -> (e => serializeEntity(e.arg2.entity)),
    "arg1Types" -> (e => serializeTypeList(e.arg1.types)),
    "arg2Types" -> (e => serializeTypeList(e.arg2.types)),
    "relLink" -> (_.rel.link.getOrElse("X")),
    "instances" -> (e => e.instances.iterator.map(t => ReVerbInstanceSerializer.serializeToString(t)).mkString("\t")))

  override def deserializeFromTokens(tokens: Seq[String]): Option[ExtractionGroup[ReVerbExtraction]] = {

    // first take all non-instance columns
    val split = tokens.take(tabDelimitedFormatSpec.length - 1).toIndexedSeq

    if (split.length != tabDelimitedFormatSpec.length - 1) {
      System.err.println("Error parsing non-instance columns: " + split); (None, tokens.drop(1))
    }

    val arg1Norm = split(0)
    val relNorm = split(1)
    val arg2Norm = split(2)
    val arg1Entity = if (split(3).equals("X")) None else deserializeEntity(split(3))
    val arg2Entity = if (split(4).equals("X")) None else deserializeEntity(split(4))
    val arg1Types = if (split(5).equals("X")) Set.empty[FreeBaseType] else deserializeTypeList(split(5))
    val arg2Types = if (split(6).equals("X")) Set.empty[FreeBaseType] else deserializeTypeList(split(6))
    val relLink = if (split(7).equals("X")) None else Some(split(7))

    var rest = tokens.drop(tabDelimitedFormatSpec.length - 1)
    var instances: List[Instance[ReVerbExtraction]] = Nil
    while (!rest.isEmpty) {
      val inst = ReVerbInstanceSerializer.deserializeFromTokens(rest)
      if (inst.isDefined) instances = inst.get :: instances
      rest = rest.drop(ReVerbExtraction.tabDelimitedColumns.length + 2)
    }

    if (instances.isEmpty) {
      System.err.println("Error: Empty set of instances!")
      None
    } else {
      val newGroup = new ExtractionGroup(arg1Norm, relNorm, arg2Norm, arg1Entity, arg2Entity, arg1Types, arg2Types, relLink, instances.toSet)
      Some(newGroup)
    }
  }

  private val commaEscapeString = Pattern.compile(Pattern.quote("|/|")) // something not likely to occur
  private val commaString = Pattern.compile(",")

  def serializeEntity(opt: Option[FreeBaseEntity]): String = opt match {

    case Some(t) => {
      val escapedName = commaString.matcher(t.name).replaceAll("|/|")
      Seq(escapedName, t.fbid, df format t.score, df format t.inlinkRatio).mkString(",")
    }
    case None => "X"
  }

  // assumes that input represents an entity that is present, not empty
  def deserializeEntity(input: String): Option[FreeBaseEntity] = {

    def fail = { System.err.println("Error parsing entity: " + input); None }

    input.split(",") match {
      case Array(escapedName, fbid, scoreStr, inlinkStr, _*) => {
        val unescapedName = commaEscapeString.matcher(escapedName).replaceAll(",")
        Some(FreeBaseEntity(unescapedName, fbid, scoreStr.toDouble, inlinkStr.toDouble))
      }
      case _ => None
    }
  }

  def serializeTypeList(types: Iterable[FreeBaseType]): String = {

    if (types.isEmpty) "X" else types.map(_.name).mkString(",")
  }

  // assumes that input represents a non-empty list
  def deserializeTypeList(input: String): Set[FreeBaseType] = {

    val split = input.split(",").filter(str => !str.isEmpty && !str.equals("Topic")) // I have no idea how "Topic" got in as a type
    val parsed = split.flatMap(str => FreeBaseType.parse(str.toLowerCase))
    if (split.length != parsed.length) System.err.println("Warning, Type parse error for: %s".format(input))
    parsed.toSet
  }

  // we use this to combine groups that share the same entities but have
  // different norm strings (e.g. tesla and nikola tesla)
  private def entityGroupingKey(group: REG): (String, String, String) = {
    val extrKeyTuple = group.instances.head.extraction.frontendGroupingKey
    (if (group.arg1.entity.isDefined) group.arg1.entity.get.fbid else extrKeyTuple._1,
      extrKeyTuple._2,
      if (group.arg2.entity.isDefined) group.arg2.entity.get.fbid else extrKeyTuple._3)
  }

  private def frontendGroupingKey(group: REG): (String, String, String) = {
    val extrKeyTuple = group.instances.head.extraction.frontendGroupingKey
    extrKeyTuple
  }

  // pass me either all groups with the same entities or one group with an entity and the rest unlinked.
  def mergeGroups(key: (String, String, String), groups: Iterable[REG]): REG = {

    if (groups.size == 0) throw new IllegalArgumentException("can't merge zero groups")
    if (groups.size == 1) return groups.head

    val linkGroup = groups.find(g=>g.arg1.hasEntity || g.arg2.hasEntity || g.rel.hasLink).getOrElse(groups.head)

    val allInstances = groups.flatMap(_.instances)
    val head = groups.head
    new ExtractionGroup(
      key._1,
      key._2,
      key._3,
      linkGroup.arg1.entity,
      linkGroup.arg2.entity,
      linkGroup.arg1.types,
      linkGroup.arg2.types,
      linkGroup.rel.link,
      allInstances.toSet)
  }



  /** Convert index key groups to frontend key groups, keeping entities together. */
  def indexGroupingToFrontendGrouping(groups: Iterable[REG]): Iterable[REG] = {
    // Assumes that input is grouped by "index" key. If not, behavior is undefined!

    // group groups by our frontendGroupingKey
    val entityGrouped = groups.groupBy(entityGroupingKey).map { case (key, keyGroups) => mergeGroups(key, keyGroups) }
    val candidateMergeGroups = entityGrouped.groupBy(frontendGroupingKey)
    val mergedCandidates = candidateMergeGroups.iterator.map { case (key, candidates) => mergeUnlinkedIntoLargestLinkedGroup(key, candidates.toSeq) }
    mergedCandidates.toSeq.flatten.map(group=>convertKey(frontendGroupingKey(group), group))
  }

  /**
    * Given a set of groups that match according to the "frontend key", decide which ones to merge!
    * If candidates contains at most one linked entity, just merge everything. Else,
    * merge only the unlinked entities.
    */
  def mergeUnlinkedIntoLargestLinkedGroup(key: (String, String, String), candidates: Seq[REG]): Seq[REG] = {

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
  private def convertKey(key: (String, String, String), group: REG): REG = {
    new ExtractionGroup(
      key._1,
      key._2,
      key._3,
      group.arg1.entity,
      group.arg2.entity,
      group.arg1.types,
      group.arg2.types,
      group.rel.link,
      group.instances)
  }


}

object ReVerbInstanceSerializer extends TabSerializer[Instance[ReVerbExtraction]] {

  import ReVerbExtractionGroup.df

  val tabDelimitedFormatSpec: List[(String, Instance[ReVerbExtraction] => String)] = List(
    "extr" -> (inst => ReVerbExtraction.serializeToString(inst.extraction)),
    "corpus" -> (inst => inst.corpus),
    "conf" -> (inst => df format inst.confidence))

  def deserializeFromTokens(tokens: Seq[String]): Option[Instance[ReVerbExtraction]] = {

    ReVerbExtraction.deserializeFromTokens(tokens).flatMap { e =>
      val split = tokens.drop(ReVerbExtraction.tabDelimitedColumns.length).take(2).toSeq
      def failure = { System.err.println("Error parsing instance tuple: " + split); None }
      if (split.length != 2) failure
      else {
        val corpus = split(0)
        val conf = split(1).toDouble
        Some(Instance(e, corpus, conf))
      }
    }
  }
}
