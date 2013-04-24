package edu.knowitall.browser.entity

import edu.knowitall.browser.hadoop.scoobi.ScoobiEntityLinker
import edu.knowitall.openie.models._
import scala.collection.mutable

class LinkerTest {

  type REG = ExtractionGroup[ReVerbExtraction]

  case class ExtractionPartStats(
    var beforeLinks: Int,
    var afterLinks: Int,
    var changedLinks: Int,
    var newLinks: Int,
    var lostLinks: Int) {

    case class Link(val arg: String, val entity: FreeBaseEntity) {
      override def toString = "(%s -> %s)".format(arg, entity.name)
    }

    val changeEvidence = new mutable.HashSet[(Link, Link)]
    val lostEvidence = new mutable.HashSet[Link]
    val newEvidence = new mutable.HashSet[Link]

    def register(beforePart: ExtractionPart, afterPart: ExtractionPart): Unit = {
      val bef = beforePart.entity.map(ent => Link(beforePart.norm, ent))
      val aft = afterPart.entity.map(ent => Link(afterPart.norm, ent))
      if (bef.isDefined) beforeLinks += 1
      if (aft.isDefined) afterLinks += 1
      if (!bef.isDefined && aft.isDefined) { newLinks += 1; newEvidence += aft.get }
      else if (bef.isDefined && !aft.isDefined) { lostLinks += 1; lostEvidence += bef.get }

      if (bef.isDefined && aft.isDefined && !beforePart.entity.get.fbid.equals(afterPart.entity.get.fbid)) {
        changedLinks += 1
        changeEvidence += ((bef.get, aft.get))
      }
    }

    def evidenceString: String = {
      "NewLinks: %s\n".format(newEvidence.map(_.toString))
        "LostLinks: %s\n".format(lostEvidence.map(_.toString))
        "ChangeLinks: %s".format(changeEvidence.map(pair => "%s ==> %s".format(pair._1.toString, pair._2.toString)))
    }

    override def toString: String = "Before: %d, After: %d, Changed: %d, New: %d, Lost: %d".format(beforeLinks, afterLinks, changedLinks, newLinks, lostLinks)
  }

  object stats {
    var totalRegs = 0
    var arg1 = ExtractionPartStats(0, 0, 0, 0, 0)
    var arg2 = ExtractionPartStats(0, 0, 0, 0, 0)

    def register(before: REG, after: REG): Unit = {
      totalRegs += 1
      arg1.register(before.arg1, after.arg1)
      arg2.register(before.arg2, after.arg2)
    }

    override def toString: String = {
      val headerString = "TotalGroups: %d\nArg1Links:[%s]\nArg2Links:[%s]".format(totalRegs, arg1.toString, arg2.toString)
      val evidenceString = "Arg1 Evidence:\n%s\nArg2 Evidence:\n%s".format(arg1.evidenceString, arg2.evidenceString)
      "%s\n%s".format(headerString, evidenceString)
    }
  }

  val linker = ScoobiEntityLinker.getEntityLinker(1)

  def runTest(inputRegs: Iterable[REG]): Unit = {

    inputRegs.foreach { before =>
      println(ReVerbExtractionGroup.serializeToString(before))
      val after = linker.linkEntities(reuseLinks = false)(before)
      stats.register(before, after)
      println(ReVerbExtractionGroup.serializeToString(after))
    }
    println(stats)
  }
}

object LinkerTest {

  import scopt.OptionParser
  import scala.io.Source

  def main(args: Array[String]): Unit = {

    var inputFiles: Seq[String] = Nil
    var maxGroups: Int = Int.MaxValue

    val parser = new OptionParser("LinkerTest") {
      arg("inputFiles", "comma-separated input files to read groups from", { str => inputFiles = str.split(",") })
      intOpt("maxGroups", "maximum groups to test per input file", { i => maxGroups = i })
    }

    if (!parser.parse(args)) return

    val sources = inputFiles.map { file => Source.fromFile(file) }
    val inputRegs = sources.flatMap { source => source.getLines flatMap ReVerbExtractionGroup.deserializeFromString take (maxGroups) }
    sources.foreach { _.close }
    (new LinkerTest).runTest(inputRegs)
  }
}