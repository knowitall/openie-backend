package edu.knowitall.browser.entity.util


import edu.knowitall.common.Resource.using
import java.io.File
import java.io.PrintWriter
import java.util.ArrayList
import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.document.Field.Index
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import scala.Array.canBuildFrom
import scala.Option.option2Iterable
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.io.Source
import scopt.OptionParser

class FbTypeLookup(val searcher: IndexSearcher, val typeIntToTypeStringMap: Map[Int, String]) {
  
  private var timeouts = 0
  
  import FbTypeLookup.badTypes
  import FbTypeLookup.memcachedTimeoutMillis
  
  // typeIntToTypeStringMap could probably just be an indexedSeq for a slight performance gain,
  // but then you have to deal with the chance that some int isn't in the enumeration separately.
  
  def this(indexPath: String, typeEnumFile: String) = 
    this(FbTypeLookup.loadIndex(indexPath), FbTypeLookup.loadEnumFile(typeEnumFile))

  def getTypesForEntity(entityFbid: String): List[String] = {
    getOrElseUpdateCache(entityFbid, getTypesForEntityUncached _)
  }
    
  /** please strip off the /m/ first. */
  private def getTypesForEntityUncached(entityFbid: String): List[String] = {
     val query = new TermQuery(new Term("fbid", entityFbid))
     val hits = searcher.search(query, null, 10)
     val rawTypes = hits.scoreDocs.map(_.doc).map(searcher.doc(_)).flatMap { doc =>
       val fbid = doc.get("fbid")
       require(fbid.equals(entityFbid))
       val typeEnumInts = doc.get("types").split(",").filter(!_.isEmpty).map(_.toInt)
       typeEnumInts.flatMap(typeIntToTypeStringMap.get(_))
     }
     
     rawTypes.filter(!badTypes.contains(_)).toList
  }
  
  private def getOrElseUpdateCache(entityFbid: String, updater: String => List[String]): List[String] = {

    return updater(entityFbid)
  }
  
  def memcachedKey(fbid: String): String = {
    "Typelookup(%s)".format(fbid)
  }
}

/** Convenience struct for helping serialize the lookup table to disk .. */
@SerialVersionUID(1337L)
case class FbPair(val entityFbid: String, val typeEnumInts: ArrayList[Int]) {
  def toDocument: Document = {
    val doc = new Document()
    doc.add(new Field("fbid", entityFbid, Store.YES, Index.NOT_ANALYZED))
    doc.add(new Field("types", typeEnumInts.mkString(","), Store.YES, Index.NO))
    doc
  }
}

object FbTypeLookup {

  import FbTypeLookupGenerator.commaRegex
import FbTypeLookupGenerator.tabRegex

  private val memcachedTimeoutMillis = 20
  
  private val badTypes = Set("Topic")
  
  def loadIndex(path: String): IndexSearcher = {
    val dir = FSDirectory.open(new File(path))
    val indexReader = IndexReader.open(dir)
    new IndexSearcher(indexReader)
  }
  
  def loadEnumFile(enumFile: String): SortedMap[Int, String] = {
    System.err.println("Loading type enumeration...")
    using(Source.fromInputStream(this.getClass.getClassLoader.getResource(enumFile).openStream())) { source =>
      val elements = source.getLines.flatMap { line =>
        tabRegex.split(line) match {
          case Array(typeInt, typeString, _*) => Some((typeInt.toInt, typeString))
          case _ => { System.err.println("Bad enum line:%s".format(line)); None }
        }
      }
      TreeMap.empty[Int, String] ++ elements.toMap
    }
  }

  def main(args: Array[String]): Unit = {
    var entityFile = ""
    var enumFile = ""
    val parser = new OptionParser() {

      arg("entityToTypeNumFile", "output file to contain entity to type enum data", { str => entityFile = str })
      arg("typeEnumFile", "output file to contain type enumeration", { str => enumFile = str })
    }
    if (!parser.parse(args)) return
    
    val lookup = new FbTypeLookup(entityFile, enumFile)

    val fbids = Seq("03gss12", "0260w54", "0260xrp", "02610rn", "02610t0")
    
    fbids.foreach(line => println("%s, %s".format(line, lookup.getTypesForEntity(line))))
  }
}

/**
  * Generates data for a type lookup table (freebase entity => freebase types)
  */
object FbTypeLookupGenerator {

  val tabRegex = "\t".r
  val commaRegex = ",".r
  val fbidPrefixRegex = "/m/".r
  val userTypeRegex = "^/user/".r

  case class ParsedLine(entityFbid: String, typeStrings: Seq[String])

  def parseLine(line: String): Option[ParsedLine] = {

    def lineFailure = { System.err.println("bad line: %s".format(line)); None }

    tabRegex.split(line) match {
      case Array(rawEntity, rawTypes, _*) => parseSplitLine(rawEntity, rawTypes)
      case Array(rawEntity) => None // some entities don't seem to have any type info associated
      case _ => lineFailure
    }
  }

  def parseSplitLine(rawEntity: String, rawTypes: String): Option[ParsedLine] = {

    // try to remove the /m/ prefix from the entity
    val trimmedEntity = fbidPrefixRegex.findFirstIn(rawEntity) match {
      case Some(string) => rawEntity.substring(3)
      case None => { System.err.println("bad entity string: %s".format(rawEntity)); return None }
    }

    // split the rawTypes by commas
    val splitTypes = commaRegex.split(rawTypes)
    // filter out types that start with /user/
    val typesNoUser = splitTypes.filter(!userTypeRegex.findFirstIn(_).isDefined)

    Some(ParsedLine(trimmedEntity, typesNoUser))
  }

  def main(args: Array[String]): Unit = {

    var entityToTypeNumFile = ""
    var typeEnumFile = ""

    val parser = new OptionParser() {

      arg("entityToTypeNumFile", "output path for lucene index", { str => entityToTypeNumFile = str })
      arg("typeEnumFile", "output file to contain type enumeration", { str => typeEnumFile = str })
    }

    if (!parser.parse(args)) return

    val parsedLines = Source.fromInputStream(System.in).getLines.flatMap(parseLine(_))

    val typesToInts = new mutable.HashMap[String, Int]
    var nextTypeInt = 0
    var linesDone = 0
    println("Reading file...")

    // convert maps to lists of entry pairs and serialize to disk.
    
    val indexWriter = getIndexWriter(entityToTypeNumFile)
    parsedLines.foreach { parsedLine =>

      val typeInts = parsedLine.typeStrings.map { typeString =>
        typesToInts.getOrElseUpdate(typeString, { val next = nextTypeInt; nextTypeInt += 1; next })
      }.sorted
      
      val enumInts = new ArrayList(typeInts)
      val fbPair = FbPair(parsedLine.entityFbid, enumInts)
      
      indexWriter.addDocument(fbPair.toDocument)
      linesDone += 1;
      if (linesDone % 100000 == 0) System.err.println("Lines done: %s".format(linesDone))
    }
    
    indexWriter.close()

    val enumWriter = new PrintWriter(typeEnumFile)

    typesToInts.iterator.toSeq.sortBy(_._2).foreach {
      case (typeString, typeInt) =>
        enumWriter.println("%s\t%s".format(typeInt, typeString))
    }

    enumWriter.flush()
    enumWriter.close()

    println("Finished.")
  }
  
  private def getIndexWriter(path: String): IndexWriter = {
    val analyzer = new WhitespaceAnalyzer(Version.LUCENE_36)
    val config = new IndexWriterConfig(Version.LUCENE_36, analyzer)
    val dir = FSDirectory.open(new File(path))
    val writer = new IndexWriter(dir, config)
    writer.setInfoStream(System.err)
    writer
  }
}