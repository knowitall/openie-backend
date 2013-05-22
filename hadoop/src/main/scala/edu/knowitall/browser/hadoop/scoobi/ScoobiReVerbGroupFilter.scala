package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text.LzoTextInput

import java.io.File
import java.io.FileWriter
import java.util.regex.Pattern

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable

import edu.knowitall.common.Timing._
import edu.knowitall.tool.postag.Postagger
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup.REG
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.Pair

object ScoobiReVerbGroupFilter extends ScoobiApp {
  final val INDEX_CONFIDENCE_THRESHOLD = 0.5
  final val MIN_GROUP_INSTANCES = 2
  final val MAX_EXTRACTION_LENGTH = 60

  private final lazy val nonQuestionableChars = Pattern.compile("[\\p{Lower}\\p{Digit} ]+")
  private final lazy val stripExtraWS = Pattern.compile("\\s+")
  private final lazy val stripChars= Pattern.compile("[^\\p{Graph}\\p{Cntrl} ]+")
  private final lazy val leadingBadChars = Pattern.compile("^\\s*(\\.|,|\\\"|\\'|\\()\\s")
  private final lazy val leadingArticle = Pattern.compile("^\\s*(the|this|these|those|that|a|an)\\s*", Pattern.CASE_INSENSITIVE)
  private final lazy val startCap = Pattern.compile(".*\\b[A-Z].*")
  private final lazy val likelyErrorPattern = Pattern.compile(".*(http|\\(|\\)|\\\"|\\[|thing).*", Pattern.CASE_INSENSITIVE)

  def run() = {
    val (inputPath, outputPath) = (args(0), args(1))

    // serialized groups
    val groups: DList[String] = LzoTextInput.fromLzoTextFile(inputPath)

    // serialized ExtractionGroup[ReVerbExtraction]
    val filtered: DList[String] = groups.flatMap  { group =>
      if (group.size > 10000000) None
      else {
        val reg = ReVerbExtractionGroup.deserializeFromString(group)
        val filteredInstances = filterInstances(reg)
        val filteredGroups = filterGroups(filteredInstances)
        filteredGroups map ReVerbExtractionGroup.serializeToString
      }
    }

    persist(TextOutput.toTextFile(filtered, outputPath + "/"));
  }

  def filterInstances(groups: Iterable[REG]): Iterable[REG] = groups.map { reg =>
      new ExtractionGroup[ReVerbExtraction](
          reg.arg1.norm,
          reg.rel.norm,
          reg.arg2.norm,
          reg.arg1.entity,
          reg.arg2.entity,
          reg.arg1.types,
          reg.arg2.types,
          reg.instances filter instanceFilterCondition(INDEX_CONFIDENCE_THRESHOLD))
    }

  def filterGroups(groups: Iterable[REG]): Iterable[REG] = groups filter groupFilterCondition filter (_.instances.size >= 2)

  private def groupFilterCondition(group: ExtractionGroup[ReVerbExtraction]): Boolean = {
    def emptyArg = group.arg1.norm.trim.isEmpty || group.rel.norm.trim.isEmpty || group.arg2.norm.trim.isEmpty

    !(emptyArg)
  }

  private def instanceFilterCondition(confThreshold: Double)(inst: Instance[ReVerbExtraction]): Boolean = {
    def clean(arg: String) = {
      var clean = this.clean(arg.trim)

      clean = leadingArticle.matcher(clean).replaceAll("");

      clean.toLowerCase
    }

    def tooShort(part: String) = {
      part.size - nonQuestionableChars.matcher(part).replaceAll("").size <= 1
    }

    val relTokens = inst.extraction.sentenceTokens.slice(inst.extraction.relInterval.start, inst.extraction.relInterval.end)
    val arg2Tokens = inst.extraction.sentenceTokens.slice(inst.extraction.arg2Interval.start, inst.extraction.arg2Interval.end)

    val arg1clean = clean(inst.extraction.arg1Text)
    val arg2clean = clean(inst.extraction.arg2Text)
    val relclean = clean(inst.extraction.relText)
    val extr = arg1clean + relclean + arg2clean

    def negative = {
      val negatives = Set("no", "not", "none", "n't", "never")
      relTokens.exists(token =>
        negatives.contains(token.string.toLowerCase)
      ) ||
      arg2Tokens.exists(token =>
        negatives.contains(token.string.toLowerCase)
      )
    }

    def tooLong =
      inst.extraction.arg1Text.length + inst.extraction.arg2Text.length + inst.extraction.relText.length > MAX_EXTRACTION_LENGTH

    def containsPronoun =
      Postagger.pronouns.contains(arg1clean) || Postagger.pronouns.contains(arg2clean)

    def likelyError =
      likelyErrorPattern.matcher(arg1clean).matches() ||
      likelyErrorPattern.matcher(arg2clean).matches()

    !(negative ||
      tooLong ||
      containsPronoun ||
      inst.confidence < confThreshold ||
      (arg1clean.isEmpty || relclean.isEmpty || arg2clean.isEmpty) ||
      (arg1clean == arg2clean) ||
      (nonQuestionableChars.matcher(extr).replaceAll("").size >= 5) ||
      (tooShort(arg1clean) || tooShort(relclean) || tooShort(arg2clean)) ||
      likelyError)
  }

  def clean(string: String) = {
    var clean = string.trim

    clean = stripChars.matcher(clean).replaceAll("");
    clean = stripExtraWS.matcher(clean).replaceAll(" ").trim();
    clean = leadingBadChars.matcher(clean).replaceAll("");

    clean
  }
}
