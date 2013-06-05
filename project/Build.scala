import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

object NlpToolsBuild extends Build {
  // settings
  val buildOrganization = "edu.washington.cs.knowitall.openie"
  val buildVersion = "1.0.0-SNAPSHOT"
  val buildScalaVersions = Seq("2.10.1")

  val mavenLocal = "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

  val nlptoolsPackage = "edu.washington.cs.knowitall.nlptools"
  val nlptoolsVersion = "2.4.1"

  val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.0.12"
  val logbackCore = "ch.qos.logback" % "logback-core" % "1.0.12"
  val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.2"

  val junit = "junit" % "junit" % "4.11"
  val specs2 = "org.specs2" %% "specs2" % "1.12.3"
  val scalatest = "org.scalatest" %% "scalatest" % "1.9.1"

  lazy val root = Project(id = "openie", base = file(".")) settings (
    crossScalaVersions := buildScalaVersions,
    scalaVersion <<= (crossScalaVersions) { versions => versions.head },
    publish := { },
    publishLocal := { }
  ) aggregate(models, populator, linker, hadoop)

  // parent build definition
  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    crossScalaVersions := buildScalaVersions,
    scalaVersion <<= (crossScalaVersions) { versions => versions.head },
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    libraryDependencies ++= Seq(junit % "test", specs2 % "test", scalatest % "test"),
    resolvers ++= Seq(
      "knowitall" at "http://knowitall.cs.washington.edu/maven2",
      "knowitall-snapshot" at "http://knowitall.cs.washington.edu/maven2-snapshot",
      mavenLocal),
    publishTo <<= version { (v: String) =>
      if (v.trim.endsWith("SNAPSHOT"))
        Some(Resolver.file("file", new File("/cse/www2/knowitall/maven2-snapshot")))
      else
        Some(Resolver.file("file", new File("/cse/www2/knowitall/maven2")))
    },
    scalacOptions ++= Seq("-unchecked", "-deprecation")
  ) ++ assemblySettings

  lazy val models = Project(id = "openie-models", base = file("models"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq(
      nlptoolsPackage %% "nlptools-core" % nlptoolsVersion,
      nlptoolsPackage %% "nlptools-stem-morpha" % nlptoolsVersion,
      "net.debasishg" %% "sjson" % "0.19",
      "com.twitter" %% "chill" % "0.2.2"
    )
  ))

  lazy val populator = Project(id = "openie-populator", base = file("populator"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.lucene" % "lucene-core" % "3.6.1",
      "org.apache.solr" % "solr-solrj" % "4.3.0",
      "net.liftweb" %% "lift-json" % "2.5-RC5",
      nlptoolsPackage %% "nlptools-stem-morpha" % nlptoolsVersion,
      nlptoolsPackage %% "nlptools-postag-opennlp" % nlptoolsVersion excludeAll(ExclusionRule(organization = "jwnl")),
      "com.google.guava" % "guava" % "14.0.1",
      logbackClassic,
      logbackCore,
      slf4jApi,
      "commons-logging" % "commons-logging-api" % "1.0.4", // solrj stupidly needs this?
      "com.github.scopt" %% "scopt" % "2.1.0",
      "net.databinder.dispatch" %% "dispatch-json4s-native" % "0.10.0",
      "net.databinder.dispatch" %% "dispatch-core" % "0.10.0")
  )) dependsOn(models)

  lazy val hadoop = Project(id = "openie-hadoop", base = file("hadoop"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq("edu.washington.cs.knowitall" % "reverb-core" % "1.4.1" excludeAll(ExclusionRule(organization = "jwnl")),
      nlptoolsPackage %% "nlptools-chunk-opennlp" % nlptoolsVersion,
      nlptoolsPackage %% "nlptools-stem-morpha" % nlptoolsVersion,
      "org.apache.hadoop" % "hadoop-lzo" % "0.4.13",
      "com.nicta" %% "scoobi" % "0.7.0-RC2-cdh3",
      logbackClassic,
      logbackCore,
      slf4jApi
    ),
    resolvers ++= Seq("nicta" at "http://nicta.github.com/scoobi/releases",
      "cloudera" at "https://repository.cloudera.com/content/repositories/releases",
      "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"),
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
      {
        case x => {
          val oldstrat = old(x)
          if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first
          else oldstrat
        }
      }
    }
  )) dependsOn(populator, linker)

  lazy val linker = Project(id = "openie-linker", base = file("linker"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq(
      "edu.washington.cs.knowitall" % "reverb-core" % "1.4.0" excludeAll(ExclusionRule(organization = "jwnl")),
      nlptoolsPackage %% "nlptools-core" % nlptoolsVersion,
      nlptoolsPackage %% "nlptools-stem-morpha" % nlptoolsVersion,
      nlptoolsPackage %% "nlptools-postag-opennlp" % nlptoolsVersion,
      "org.apache.lucene" % "lucene-core" % "3.0.3",
      "org.apache.lucene" % "lucene-queries" % "3.0.3",
      "org.apache.lucene" % "lucene-core" % "3.6.0",
      "com.github.scopt" %% "scopt" % "2.1.0",
      logbackClassic,
      logbackCore,
      slf4jApi,
      junit,
      scalatest,
      "org.apache.derby" % "derby" % "10.9.1.0",
      "org.apache.derby" % "derbyclient" % "10.9.1.0"
    )
  )) dependsOn(models)
}
