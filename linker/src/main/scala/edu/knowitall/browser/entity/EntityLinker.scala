package edu.knowitall.browser.entity

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream
import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.HashMap
import scala.io.Source

import edu.knowitall.common.Resource.using
import scopt.OptionParser

//import edu.knowitall.browser.hadoop.scoobi.EntityTyper

class EntityLinker(val bm: batch_match, val candidateFinder: CandidateFinder,
    val typer: EntityTyper) {
  private val PAD_SOURCES = 4 // extend source sentences to this
  // number minimum

  private var totalLookups = 0
  private var cacheHits = 0
  private var cacheTimeouts = 0

  def this(basePath: File) = this(
    new batch_match(basePath),
    new CrosswikisCandidateFinder(basePath),
    new EntityTyper(basePath)
  )

  private def tryFbidCache(arg: String): Seq[Pair[String, java.lang.Double]] =
    candidateFinder.linkToFbids(arg)

  def getBestEntity(arg: String, sourceSentences: Seq[String]): Option[EntityLink] = getBestEntities(arg, sourceSentences).headOption
  
  def getBestEntities(arg: String, sourceSentences: Seq[String]): Seq[EntityLink] = {

    getBestFbidsFromSources(arg, sourceSentences).map(typer.typeEntity)
  }

  /**
    * returns null for none! Returns an entity without types attached.
    *
    * @param arg
    * @param sources
    * @return
    * @throws IOException
    * @throws ClassNotFoundException
    */
  private def getBestFbidsFromSources(arg: String, inputSources: Seq[String]): Seq[EntityLink] = {

    var sources = inputSources

    if (sources.isEmpty()) {
      System.err.println("Warning: no source sentences for arg: " + arg);
      return Seq.empty; // later code assumes that we don't have any empty list
    }
    totalLookups += 1
    val fbidPairs = tryFbidCache(arg)

    if (totalLookups % 20000 == 0)
      System.err.println("Linker lookups: " + totalLookups
        + " cache hits: " + cacheHits + " cache timeouts: "
        + cacheTimeouts)

    if (fbidPairs.isEmpty()) return Seq.empty

    while (sources.size() < PAD_SOURCES) {
      val newSources = new ArrayList[String](PAD_SOURCES)
      newSources.addAll(sources);
      newSources.addAll(sources);

      sources = newSources;
    }

    val fbids = fbidPairs.map(pair => pair.one).toList
    val fbidScores = bm.processSingleArgWithSources(arg, fbids, sources).toIterable

    return getBestFbids(arg, fbidPairs, fbidScores);
  }

  /**
    * Return (title, fbid) for the best entity match,
    *
    * returns null for none
    *
    * @param arg1
    * @param fbidScores
    * @return
    * @throws ClassNotFoundException
    * @throws IOException
    * @throws FileNotFoundException
    */EntityLinker
  private def getBestFbids(arg: String, fbidPairs: Seq[Pair[String, java.lang.Double]], fbidScores: Iterable[Pair[String, java.lang.Double]]): Seq[EntityLink] = {
    var bestCombinedScore = Double.NegativeInfinity
    var bestTitle = ""
    var bestFbid = ""
    var bestCandidateScore = 0.0
    var bestDocSimScore = 0.0
    var bestInlinks = 0

    var bestLinks = List.empty[EntityLink]
    
    var fbidScoresEmpty = true

    val fbidCprobs = HashMap[String, java.lang.Double]();
    for (fbidPair <- fbidPairs) {
      val fbid = fbidPair.one
      val cprob = fbidPair.two
      fbidCprobs += (fbid -> cprob)
    }

    for (fbidScore <- fbidScores) {
      fbidScoresEmpty = false;
      val titleInlinks = candidateFinder.getTitleInlinks(fbidScore.one)
      val title = titleInlinks.one
      val inlinks = titleInlinks.two
      val cprob = fbidCprobs.get(fbidScore.one) match {
        case Some(cprob) =>
          bestLinks ::= new EntityLink(title, fbidScore.one, cprob, inlinks, fbidScore.two)
          
        case None => { System.err.println("Warning, candidate score not present.") }
      }
    }

    bestLinks.sortBy(-_.combinedScore)
  }

  private def scoreFbid(arg: String, title: String, cprob: Double, inlinks: Int, score: Double): Double = {
    return cprob * math.log(inlinks) * score
  }
}

object EntityLinker {

  private val tabSplitter = "\t".r

  type Args = Seq[String]
  type Context = Seq[String]

  // input format: arg:X	arg:X	sent1	sent2	setn3
  // output format: fbid1	fbid2	fbid3	...
  // where fbidX has 5 columns of name,fbid,score,inlinks,types
  def main(args: Array[String]): Unit = {

    var baseDir = "."
    var inputFile = Option.empty[String]
    var outputFile = Option.empty[String]
    var numArgs = 1

    val parser = new OptionParser() {
      arg("num_args", "Number of input columns that are arg strings to link, any remaining columns are treated as context sentences.", { str => numArgs = str.toInt })
      opt("baseDir", "The base directory for linker support files. Default: .", { str => baseDir = str })
      opt("inputFile", "An optional input file to read input from, default standard input", { str => inputFile = Some(str) })
      opt("outputFile", "An optional output file to write output to, default standard output", { str => outputFile = Some(str) })
    }

    if (!parser.parse(args)) return

    val baseDirFile = new File(baseDir)
    
    val linker = new EntityLinker(
      new batch_match(baseDirFile),
      new CrosswikisCandidateFinder(baseDirFile, 0.01, 10),
      new EntityTyper(baseDirFile))

    def parseInputLine(line: String): (Args, Context) = {
      val split = tabSplitter.split(line)
      (split.take(numArgs), split.drop(numArgs))
    }

    def linkLine(line: String): String = {
      val (args, context) = parseInputLine(line)
      val entities = args.flatMap(arg => linker.getBestEntities(arg, context).map(link => (arg, link)))
      def entityToTabbed(argEntity: (String, EntityLink)) = {
        val link = argEntity._2
        Seq(link.entity.name, link.entity.fbid, link.candidateScore, link.inlinks, link.docSimScore, link.combinedScore, link.retrieveTypes.mkString(",")).mkString("\t")
      }
      val entityStrings = entities map entityToTabbed
      entityStrings.mkString("\n")
    }

    def getInput: Source = inputFile match {
      case Some(filename) => Source.fromFile(filename)
      case None => Source.stdin
    }

    def getOutput: PrintStream = outputFile match {
      case Some(filename) => new PrintStream(new FileOutputStream(filename))
      case None => System.out
    }

    using(getInput) { input =>
      using(getOutput) { output =>
        def printLine(line: String) = output.println(line)
        getInput.getLines map linkLine foreach printLine
      }
    }
  }
}
