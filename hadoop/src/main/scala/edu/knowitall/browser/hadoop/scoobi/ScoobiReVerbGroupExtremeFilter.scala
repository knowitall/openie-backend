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
import edu.knowitall.openie.models.ReVerbExtraction
import edu.knowitall.openie.models.Instance
import edu.knowitall.openie.models.ExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup
import edu.knowitall.openie.models.ReVerbExtractionGroup.REG
import edu.knowitall.browser.entity.EntityLinker
import edu.knowitall.browser.entity.Pair
import com.nicta.scoobi.io.text.TextOutput
import com.nicta.scoobi.io.text.TextInput
import com.nicta.scoobi.io.text.TextSource
import com.hadoop.mapreduce.LzoTextInputFormat
import edu.knowitall.tool.stem.MorphaStemmer
import edu.knowitall.tool.chunk.ChunkedToken

object ScoobiReVerbGroupExtremeFilter extends ScoobiApp {
  def run() = {
    val (inputPath, outputPath) = (args(0), args(1))

    // serialized groups
    val groups: DList[String] = TextInput.fromTextSource(new TextSource(Seq(inputPath),  inputFormat = classOf[LzoTextInputFormat].asInstanceOf[Class[org.apache.hadoop.mapreduce.lib.input.TextInputFormat]]))

    // serialized ExtractionGroup[ReVerbExtraction]
    val filtered: DList[String] = groups.flatMap  { group =>
      if (group.size > 10000) None
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
          reg.instances filter instanceFilterCondition(0.9))
    }

  def filterGroups(groups: Iterable[REG]): Iterable[REG] = groups filter groupFilterCondition filter (_.instances.size >= 2)

  private def groupFilterCondition(group: ExtractionGroup[ReVerbExtraction]): Boolean = {
    group.instances.size > 0
  }

  private def instanceFilterCondition(confThreshold: Double)(inst: Instance[ReVerbExtraction]): Boolean = {
    def definiteNoun(tokens: Seq[ChunkedToken]): Boolean = {
      var tokensLeft = tokens
      while (!tokensLeft.isEmpty) {
        tokensLeft = tokensLeft.dropWhile(_.postag != "DT")
        if (!tokensLeft.isEmpty && (tokensLeft.head.postag == "NN" || tokensLeft.head.postag == "NNP")) {
          return true
        }

        tokensLeft = tokensLeft.drop(1)
      }

      return false
    }
    val relationBlacklist = Set("said", "have", "is")
    val argumentBlacklist = Set("both", "all", "some", "other", "this", "that", "those",
        "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "yesterday", "tomorrow", "today")
    inst.confidence > confThreshold &&
      !(inst.extraction.relTokens.size == 1 && inst.extraction.relTokens.exists(token => relationBlacklist(MorphaStemmer.lemmatize(token.string)))) &&
      !(inst.extraction.arg1Tokens.exists(token => argumentBlacklist(MorphaStemmer.lemmatize(token.string)))) &&
      !(inst.extraction.arg2Tokens.exists(token => argumentBlacklist(MorphaStemmer.lemmatize(token.string)))) &&
      !definiteNoun(inst.extraction.arg1Tokens) &&
      !definiteNoun(inst.extraction.arg2Tokens)
  }
}
