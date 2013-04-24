import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

object NlpToolsBuild extends Build {
  // settings
  val buildOrganization = "edu.washington.cs.knowitall.openie.backend"
  val buildVersion = "1.0.0-SNAPSHOT"
  val buildScalaVersions = Seq("2.9.3")

  val mavenLocal = "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

  val nlptoolsPackage = "edu.washington.cs.knowitall.nlptools"
  val nlptoolsVersion = "2.4.1"

  val junit = "junit" % "junit" % "4.11"
  val specs2 = "org.specs2" % "specs2_2.9.2" % "1.12.3"
  val scalatest = "org.scalatest" %% "scalatest" % "1.9.1"

  lazy val root = Project(id = "openie", base = file(".")) settings (
    crossScalaVersions := buildScalaVersions,
    scalaVersion <<= (crossScalaVersions) { versions => versions.head },
    publish := { },
    publishLocal := { }
  ) aggregate(backend, hadoop)

  // parent build definition
  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    crossScalaVersions := buildScalaVersions,
    scalaVersion <<= (crossScalaVersions) { versions => versions.head },
    libraryDependencies ++= Seq(junit % "test", specs2 % "test", scalatest % "test"),
    resolvers ++= Seq(
      "knowitall" at "http://knowitall.cs.washington.edu/maven2",
      "knowitall-snapshot" at "http://knowitall.cs.washington.edu/maven2-snapshot",
      mavenLocal),
    scalacOptions ++= Seq("-unchecked", "-deprecation")
  ) ++ assemblySettings

  lazy val backend = Project(id = "openie-backend", base = file("backend"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq(
      "org.apache.lucene" % "lucene-core" % "3.6.1",
      "net.debasishg" % "sjson_2.9.2" % "0.19",
      nlptoolsPackage % "nlptools-stem-morpha_2.9.2" % nlptoolsVersion,
      nlptoolsPackage % "nlptools-postag-opennlp_2.9.2" % nlptoolsVersion,
      "com.google.guava" % "guava" % "14.0.1",
      "ch.qos.logback" % "logback-classic" % "1.0.7",
      "org.slf4j" % "slf4j-api" % "1.7.2",
      "ch.qos.logback" % "logback-core" % "1.0.7",
      "com.github.scopt" % "scopt_2.9.2" % "2.1.0",
      "net.databinder.dispatch" %% "dispatch-json4s-native" % "0.10.0",
      "net.databinder.dispatch" %% "dispatch-core" % "0.10.0")
  ))

  lazy val hadoop = Project(id = "openie-hadoop", base = file("hadoop"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq("edu.washington.cs.knowitall" % "reverb-core" % "1.4.1",
      nlptoolsPackage % "nlptools-chunk-opennlp_2.9.2" % nlptoolsVersion,
      nlptoolsPackage % "nlptools-stem-morpha_2.9.2" % nlptoolsVersion,
      "com.nicta" % "scoobi_2.9.2" % "0.6.0-cdh3",
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % "1.6.3",
      "org.slf4j" % "slf4j-log4j12" % "1.6.3"
    ),
    resolvers ++= Seq("nicta" at "http://nicta.github.com/scoobi/releases",
      "cloudera" at "https://repository.cloudera.com/content/repositories/releases")
  )) dependsOn(backend, linker)

  lazy val linker = Project(id = "openie-linker", base = file("linker"), settings = buildSettings ++ Seq(
    libraryDependencies ++= Seq(
      "edu.washington.cs.knowitall" % "reverb-core" % "1.4.0",
      nlptoolsPackage % "nlptools-core_2.9.2" % "2.3.1-SNAPSHOT",
      nlptoolsPackage % "nlptools-stem-morpha_2.9.2" % nlptoolsVersion,
      nlptoolsPackage % "nlptools-postag-opennlp_2.9.2" % nlptoolsVersion,
      "org.apache.lucene" % "lucene-core" % "3.0.3",
      "org.apache.lucene" % "lucene-queries" % "3.0.3",
      "org.apache.lucene" % "lucene-core" % "3.6.0",
      "com.github.scopt" % "scopt_2.9.2" % "2.1.0",
      "log4j" % "log4j" % "1.2.17",
      "org.slf4j" % "slf4j-api" % "1.7.2",
      "org.slf4j" % "slf4j-log4j12" % "1.7.2",
      "junit" % "junit" % "4.10",
      "org.scalatest" % "scalatest_2.9.2" % "1.7.1",
      "org.apache.derby" % "derby" % "10.9.1.0"
    )
  )) dependsOn(backend)
}
