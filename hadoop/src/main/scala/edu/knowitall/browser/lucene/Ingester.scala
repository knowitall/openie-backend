package edu.knowitall.browser.lucene

import scala.sys.process._
import java.io.File
import java.io.FileInputStream
import edu.knowitall.common.Resource.using
import edu.knowitall.common.Timing
import edu.knowitall.openie.models.ReVerbExtraction

import edu.knowitall.tool.tokenize.OpenNlpTokenizer

/**
  * Examines the differences between the files in a local dir and an hdfs dir. Files not in
  * the hdfs dir are both added to the hdfs dir, and also sent to ParallelReVerbIndexModifier
  */
class Ingester(
  val indexModifier: IndexModifier,
  val converterJar: String,
  val hadoopDir: String,
  val localDir: String,
  val localDirHost: String,
  val sshIdentityKeyFile: String,
  val corpus: String,
  val verbose: Boolean = false) {

  import Ingester.printErr
  import ParallelReVerbIndexModifier.extrToSingletonGroup

  private var extractionsIngested = 0

  private val whiteSpaceRegex = "\\s+".r

  // These are particular to the RV cluster
  private val hadoopCmd = "/home/knowall/hadoop/bin/hadoop"
  private val sshCmd = "/usr/bin/ssh"
  private val lzopCmd = "/usr/bin/lzop"
  private val javaCmd = "/usr/bin/java"

  private def stripPathAndExt(fileName: String): String = {
    val noPath = fileName.drop(fileName.lastIndexOf(File.separatorChar) + 1)
    val noExt = noPath.take(noPath.indexOf("""."""))
    noExt
  }

  private def filesInHadoopDir: Set[String] = {

    val fullHadoopCmd = "%s dfs -ls %s".format(hadoopCmd, hadoopDir)
    printErr("Executing command: %s".format(fullHadoopCmd))
    val fullPathNames = Process(fullHadoopCmd).lines.flatMap { cmdOutputLine =>
      val split = whiteSpaceRegex.split(cmdOutputLine)
      split match {
        case Array(attrs, repl, owner, group, size, dateYmd, time, fullName, _*) => {
          if (size.toInt > 0) Some(fullName) else None
        }
        case _ => None
      }
    }

    val hdfsFiles: Set[String] = fullPathNames map stripPathAndExt toSet;
    printErr("%d files in hdfs directory".format(hdfsFiles.size))
    if (verbose) hdfsFiles foreach printErr
    hdfsFiles
  }

  private def filesInLocalDir: Map[String, File] = {

    val cmd = "%s -i %s %s ls -1 %s".format(sshCmd, sshIdentityKeyFile, localDirHost, localDir)
    printErr("Executing command: %s".format(cmd))
    val localFileMap: Map[String, File] = Process(cmd).lines map { fileName => (stripPathAndExt(fileName), new File(fileName)) } toMap;
    printErr("%d files in local directory".format(localFileMap.size));
    if (verbose) localFileMap.iterator foreach { case (key, value) => printErr("%s -> %s".format(key, value)) }
    localFileMap
  }

  private def filesNotInHadoop: Map[String, File] = filesInLocalDir -- filesInHadoopDir

  private def extrFilter(extr: ReVerbExtraction): Boolean = {
    try {
      // doing this helps make sure the extraction has valid indices.
      val silent = ("(%s) %s".format(extr.indexGroupingKeyString, extr.toString))
      // printErr(silent) 
      true
    } catch {
      case e: Exception => { e.printStackTrace; false }
    }
  }

  private def ingestHdfsToIndex(hdfsFile: String): Unit = {
    val hdfsCmd = "%s dfs -cat %s".format(hadoopCmd, hdfsFile)
    val lzopFullCmd = "%s -cd".format(lzopCmd)
    printErr("Executing command: %s | %s".format(hdfsCmd, lzopFullCmd))
    val extrs = (Process(hdfsCmd) #| Process(lzopFullCmd)).lines.iterator flatMap ReVerbExtraction.deserializeFromString
    val groups = extrs filter extrFilter map extrToSingletonGroup(corpus) toSeq;
    extractionsIngested += groups.size
    indexModifier.updateAll(groups.iterator)
  }

  private def ingestFileToHdfs(file: File): String = {
    val remoteCatCmd = "%s -i %s %s cat %s/%s".format(sshCmd, sshIdentityKeyFile, localDirHost, localDir, file.getName)
    val convertCmd = "%s -jar %s".format(javaCmd, converterJar)
    val compressCmd = "%s -c".format(lzopCmd)
    val hadoopFile = "%s/%s".format(hadoopDir, file.getName + ".lzo")
    val hdfsPutCmd = "%s dfs -put - %s".format(hadoopCmd, hadoopFile)
    printErr("Executing command: %s | %s | %s | %s".format(remoteCatCmd, convertCmd, compressCmd, hdfsPutCmd))
    remoteCatCmd #> convertCmd #| compressCmd #| hdfsPutCmd !

    hadoopFile
  }

  def run: Unit = {
    val filesToIngest = filesNotInHadoop.iterator.toSeq
    printErr("Ingesting %d files".format(filesToIngest.size))
    filesToIngest foreach {
      case (name, file) =>
        printErr("Ingesting %s into HDFS".format(name))
        val hadoopFile = ingestFileToHdfs(file)
        printErr("Ingesting %s into index".format(name))
        ingestHdfsToIndex(hadoopFile)
    }
    printErr("Ingestion complete, %d extractions total extractions ingested from %d files".format(extractionsIngested, filesToIngest.size))
  }
}

object Ingester {

  private def printErr(line: String): Unit = System.err.println(line)

  def main(args: Array[String]): Unit = {

    {
      val logger: org.slf4j.Logger =
        org.slf4j.LoggerFactory.getLogger("test.package");
      if ((logger.isInstanceOf[ch.qos.logback.classic.Logger])) {
        val logbackLogger: ch.qos.logback.classic.Logger =
          logger.asInstanceOf[ch.qos.logback.classic.Logger]
        logbackLogger.setLevel(ch.qos.logback.classic.Level.ERROR)
      }
    }

    var indexPaths: Seq[String] = Nil
    var ramBufferMb: Int = 500
    var linesPerCommit: Int = 25000
    var converterJar: String = ""
    var hadoopDir: String = ""
    var localDir: String = ""
    var localDirHost: String = ""
    var sshIdentityKeyFile: String = ""
    var corpus: String = ""
    var verbose: Boolean = false

    val parser = new scopt.OptionParser() {
      arg("indexPaths", "Path to parallel lucene indexes, colon separated", { str => indexPaths = str.split(":") })
      arg("converterJar", "full path to David Jung's ReVerb format converter jar", { converterJar = _ })
      arg("localDirHost", "hostname where JSON ReVerb is located, e.g. rv-n14", { localDirHost = _ })
      arg("localDir", "directory on localDirHost where JSON data is located", { localDir = _ })
      arg("hadoopDir", "hdfs directory for converted reverb data", { hadoopDir = _ })
      arg("sshKey", "ssh identity to use for ssh-ing to localDirHost", { sshIdentityKeyFile = _ })
      arg("corpus", "corpus identifier to attach to new data in the index", { corpus = _ })
      intOpt("ramBufferMb", "ramBuffer in MB per index", { ramBufferMb = _ })
      intOpt("linesPerCommit", "num lines between index commits", { linesPerCommit = _ })
      opt("v", "verbose", { verbose = true })
    }

    if (!parser.parse(args)) return else printErr("Parsed args: %s".format(args.mkString(", ")))

    val indexModifier = new ParallelReVerbIndexModifier(
      indexPaths = indexPaths,
      ramBufferMb = ramBufferMb,
      linesPerCommit = linesPerCommit)

    val ingester = new Ingester(
      indexModifier = indexModifier,
      converterJar = converterJar,
      hadoopDir = hadoopDir,
      localDir = localDir,
      localDirHost = localDirHost,
      sshIdentityKeyFile = sshIdentityKeyFile,
      corpus = corpus,
      verbose = verbose)

    val nsRuntime = Timing.time {
      try { ingester.run }
      finally { indexModifier.close }
    }
    println("Run time: %s".format(Timing.Seconds.format(nsRuntime)))
  }
}