package edu.knowitall.browser.lucene

import java.util.concurrent.ArrayBlockingQueue

import scala.Array.canBuildFrom
import scala.Option.option2Iterable

import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.index.Term
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.search.{BooleanClause, BooleanQuery, NumericRangeQuery, Query, TermQuery}
import org.apache.lucene.util.Version
import org.slf4j.LoggerFactory

import edu.knowitall.common.Timing
import edu.knowitall.openie.models.{ExtractionGroup, ReVerbExtraction}
import edu.knowitall.openie.models.ReVerbExtraction.strippedDeterminers
import edu.knowitall.openie.models.util.TaggedStemmer
import edu.knowitall.tool.postag.OpenNlpPostagger
import edu.knowitall.tool.tokenize.OpenNlpTokenizer

case class QuerySpec(
  val arg1: Option[String],
  val rel: Option[String],
  val arg2: Option[String],
  val arg1Entity: Option[String],
  val arg2Entity: Option[String],
  val arg1Types: Option[String] = None,
  val arg2Types: Option[String] = None,
  val corpora: Option[String] = None,
  val stem: Boolean = true,
  val and: Boolean = true,
  val arg1Fbid: Option[String] = None,
  val arg2Fbid: Option[String] = None) {

  def defaultOp = if (and) QueryParser.AND_OPERATOR else QueryParser.OR_OPERATOR

  import ReVerbExtraction.strippedDeterminers

  lazy val lowLevelLuceneQueries: Seq[Query] = {
    numericRangeQueries.map { rangeQuery =>
      val booleanQuery = new BooleanQuery(true)
      booleanQuery.add(luceneQuery, BooleanClause.Occur.MUST)
      booleanQuery.add(rangeQuery, BooleanClause.Occur.MUST)
      booleanQuery
    }
  }

  lazy val luceneQuery: Query = {
    val parser = QuerySpec.parsers.take()
    try {
      parser.setDefaultOperator(defaultOp)
      val parse = parser.parse(luceneQueryString)

      parse
    } finally {
      QuerySpec.parsers.add(parser)
    }
  }

  def relNorm = rel map stemAllTokens

  private def stemAllTokens(line: String) = {
    QuerySpec.logger.debug("stem tokens: " + line)
    val stemmer = TaggedStemmer.instance
    val postagger = QuerySpec.taggers.take()
    try {
      if (stem) {
        val tokens = postagger.postag(line)
        val stemmed = stemmer.stemAll(tokens)
        val filtered = stemmed.filter(!strippedDeterminers.contains(_))
        filtered.mkString(" ")
      } else {
        line
      }
    } finally {
      QuerySpec.taggers.add(postagger)
    }
  }

  lazy val luceneQueryString: String = {

    def escape(line: String) = QueryParser.escape(line.replaceAll("\"", ""))

    var arg1Clauses, relClauses, arg2Clauses = List.empty[String]
    var arg1TypeClause, arg2TypeClause, corporaClause = List.empty[String]

    if (arg1.isDefined) {
      val arg1Clean = escape(arg1.get)
      val arg1Norm = stemAllTokens(arg1Clean).toLowerCase
      arg1Clauses ::= "arg1Norm:\"%s\"".format(arg1Norm)
      if (!arg1Norm.equals(arg1Clean)) {
        arg1Clauses ::= "arg1Norm:\"%s\"".format(arg1Clean)
      }
    }

    if (arg1Fbid.isDefined) arg1Clauses ::= "arg1EntityId:\"%s\"".format(arg1Fbid.get)

    if (arg1Entity.isDefined) arg1Clauses ::= "arg1EntityName:\"%s\"".format(QueryParser.escape(arg1Entity.get))

    if (arg1Types.isDefined) arg1TypeClause ::= "arg1Types:\"%s\"".format(escape(arg1Types.get))

    if (rel.isDefined) {
      val relClean = escape(rel.get)
      val relNorm = stemAllTokens(relClean).toLowerCase
      relClauses ::= "relNorm:\"%s\"".format(relNorm)
    }

    if (arg2.isDefined) {
      val arg2Clean = escape(arg2.get)
      val arg2Norm = stemAllTokens(arg2Clean).toLowerCase
      arg2Clauses ::= "arg2Norm:\"%s\"".format(arg2Norm)
      if (!arg2Norm.equals(arg2Clean)) {
        arg2Clauses ::= "arg2Norm:\"%s\"".format(arg2Clean)
      }
    }

    if (arg2Fbid.isDefined) arg2Clauses ::= "arg2EntityId:\"%s\"".format(arg2Fbid.get)

    if (arg2Entity.isDefined) arg2Clauses ::= "arg2EntityName:\"%s\"".format(QueryParser.escape(arg2Entity.get))

    if (arg2Types.isDefined) arg2TypeClause ::= "arg2Types:\"%s\"".format(escape(arg2Types.get).toLowerCase)

    if (corpora.isDefined) corporaClause ::= corpora.get.split(" ").map(corpus => "corpora:\"%s\"".format(escape(corpus).toLowerCase)).mkString(" OR ")

    val clauses = Seq(arg1Clauses, relClauses, arg2Clauses, arg1TypeClause, arg2TypeClause, corporaClause).filter(!_.isEmpty)
    val disjunctions = clauses.map(clause => "(%s)".format(clause.mkString(" OR ")))
    val joinString = if (and) " AND " else " OR "
    val fullQueryString = disjunctions.mkString(joinString)

    QuerySpec.logger.info("Computed lucene query string: %s".format(fullQueryString))

    fullQueryString
  }

  private def numSpecifiedFields = Seq(arg1, rel, arg2, arg1Entity, arg2Entity, arg1Types, arg2Types).flatten.size

  private def numericRangeQueries: Seq[NumericRangeQuery[java.lang.Integer]] = {

    if (numSpecifiedFields <= 1) {
      Seq(NumericRangeQuery.newIntRange("size", 1, 4, true, false),
          NumericRangeQuery.newIntRange("size", 4, 25, true, false),
          NumericRangeQuery.newIntRange("size", 25, 50, true, false),
          NumericRangeQuery.newIntRange("size", 50, null, true, true)).reverse
    } else {
      Seq(NumericRangeQuery.newIntRange("size", 1, 4, true, false),
          NumericRangeQuery.newIntRange("size", 4, 15, true, false),
          NumericRangeQuery.newIntRange("size", 15, null, true, true)).reverse
    }
  }
}

private class GroupIdentityQuerySpec(
    val group: ExtractionGroup[ReVerbExtraction],
  arg1: Option[String],
  rel: Option[String],
  arg2: Option[String],
  arg1Entity: Option[String],
  arg2Entity: Option[String],
  arg1Types: Option[String],
  arg2Types: Option[String],
  corpora: Option[String],
  stem: Boolean,
  and: Boolean) extends QuerySpec(arg1, rel, arg2, arg1Entity, arg2Entity, arg1Types, arg2Types, corpora, stem, and) {

  override lazy val luceneQuery = {

    val doc = ReVerbDocumentConverter.toDocument(group)

    def getTerm(field: String) = doc.getFieldable(field).stringValue.split(" ").map(_.trim).filter(!_.isEmpty).map(t=>new Term(field, t))

    val terms = Seq(getTerm("arg1Norm"), getTerm("relNorm"), getTerm("arg2Norm")).flatten

    val termQueries = terms.map(new TermQuery(_))

    val booleanQuery = new BooleanQuery(true)
    val occur = BooleanClause.Occur.MUST
    termQueries.foreach(booleanQuery.add(_, occur))
    booleanQuery
  }

  override lazy val lowLevelLuceneQueries = Seq(luceneQuery)
}

object QuerySpec {
  val logger = LoggerFactory.getLogger(this.getClass)

  protected val whitespaceSplitter = "\\s+".r

  val taggers = {
    val capacity = 4

    Timing.timeThen {
    val queue = new ArrayBlockingQueue[OpenNlpPostagger](capacity)

    val tokenizer = new OpenNlpTokenizer()
    val tagger = new OpenNlpPostagger(tokenizer=tokenizer)
    queue.add(tagger)
    for (i <- 1 to (capacity - 1)) {
      val newTokenizer = new OpenNlpTokenizer(tokenizer.model)
      val newTagger = new OpenNlpPostagger(tagger.model, newTokenizer)
      queue.add(newTagger)
    }

    queue
    } (ns => QuerySpec.logger.debug("Initialized " + capacity + " POS taggers (" + Timing.Seconds.format(ns) + ")"))
  }

  val parsers = {
    val capacity = 4

    Timing.timeThen {
      val queue = new ArrayBlockingQueue[QueryParser](capacity)

      for (i <- 1 to capacity) {
        val qparser = new QueryParser(Version.LUCENE_36, "relNorm", new WhitespaceAnalyzer(Version.LUCENE_36));
        qparser.setDefaultOperator(QueryParser.AND_OPERATOR)

        queue.add(qparser)
      }

      queue
    }(ns => QuerySpec.logger.debug("Initialized " + capacity + " Query parsers (" + Timing.Seconds.format(ns) + ")"))
  }

  def identityQuery(group: ExtractionGroup[ReVerbExtraction]): QuerySpec = {
    new GroupIdentityQuerySpec(group, Some(group.arg1.norm), Some(group.rel.norm), Some(group.arg2.norm), None, None, None, None, None, true, true)
  }

  val warmupQueries: Seq[QuerySpec] = Seq(
    QuerySpec(None, Some("kill"), Some("bacteria"), None, None),  // "what kills bacteria?"
    QuerySpec(Some("FDA"), Some("approve"), None, None, None, Some("drug")), // what drugs has the FDA approved?
    QuerySpec(None, Some("contain"), Some("antioxidant"), None, None, Some("food")) // what foods contain antioxidants?
  )

}
