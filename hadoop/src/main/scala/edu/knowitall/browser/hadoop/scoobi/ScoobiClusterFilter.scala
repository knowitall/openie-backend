package edu.knowitall.browser.hadoop.scoobi

import com.nicta.scoobi.Scoobi._
import java.io.File
import java.io.FileWriter
import java.util.regex.Pattern
import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable
import edu.knowitall.common.Timing._
import edu.knowitall.tool.postag.Postagger
import edu.knowitall.openie.models.Extraction
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.openie.models.Extraction
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.Pair
import com.nicta.scoobi.io.text.TextOutput
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat
import edu.knowitall.openie.models.ExtractionCluster
import edu.knowitall.tool.stem.MorphaStemmer

object ScoobiClusterFilter {
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

  def filterInstances(clusters: Iterable[ExtractionCluster[Extraction]]): Iterable[ExtractionCluster[Extraction]] = clusters.map { reg =>
      new ExtractionCluster[Extraction](
          reg.arg1.norm,
          reg.rel.norm,
          reg.arg2.norm,
          reg.arg1.entity,
          reg.arg2.entity,
          reg.arg1.types,
          reg.arg2.types,
          reg.instances filter instanceFilterCondition(INDEX_CONFIDENCE_THRESHOLD))
    }

  def filterClusters(clusters: Iterable[ExtractionCluster[Extraction]]): Iterable[ExtractionCluster[Extraction]] = clusters filter clusterFilterCondition filter (_.instances.size >= 2)

  def clusterFilterCondition(cluster: ExtractionCluster[Extraction]): Boolean = {
    def emptyArg = cluster.arg1.norm.trim.isEmpty || cluster.rel.norm.trim.isEmpty || cluster.arg2.norm.trim.isEmpty

    !(emptyArg)
  }

  def instanceFilterCondition(confThreshold: Double)(inst: Extraction): Boolean = {
    def clean(arg: String) = {
      var clean = this.clean(arg.trim)

      clean = leadingArticle.matcher(clean).replaceAll("");

      clean.toLowerCase
    }

    def tooShort(part: String) = {
      part.size - nonQuestionableChars.matcher(part).replaceAll("").size <= 1
    }

    val relTokens = inst.sentenceTokens.slice(inst.relInterval.start, inst.relInterval.end)
    val arg2Tokens = inst.sentenceTokens.slice(inst.arg2Interval.start, inst.arg2Interval.end)

    val arg1clean = clean(inst.arg1Text)
    val arg2clean = clean(inst.arg2Text)
    val relclean = clean(inst.relText)
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
      inst.arg1Text.length + inst.arg2Text.length + inst.relText.length > MAX_EXTRACTION_LENGTH

    def containsPronoun =
      Postagger.pronouns.contains(arg1clean) || Postagger.pronouns.contains(arg2clean)

    def likelyError =
      likelyErrorPattern.matcher(arg1clean).matches() ||
      likelyErrorPattern.matcher(arg2clean).matches()

    val argumentBlacklist = Set("both", "other", "this", "that", "those",
      "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "yesterday", "tomorrow", "today")

    !(negative ||
      tooLong ||
      containsPronoun ||
      inst.confidence < confThreshold ||
      (arg1clean.isEmpty || relclean.isEmpty || arg2clean.isEmpty) ||
      (arg1clean == arg2clean) ||
      (nonQuestionableChars.matcher(extr).replaceAll("").size >= 5) ||
      (tooShort(arg1clean) || tooShort(relclean) || tooShort(arg2clean)) ||
      (inst.arg1Tokens.exists(token => argumentBlacklist(token.string.toLowerCase))) ||
      (inst.arg2Tokens.exists(token => argumentBlacklist(token.string.toLowerCase))) ||
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
