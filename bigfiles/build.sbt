import Dependencies.*
import sbt.Package.ManifestAttributes
import sbtassembly.MergeStrategy

import java.time.LocalDateTime

enablePlugins(GitVersioning, GitBranchPrompt)
enablePlugins(ScalafmtPlugin)

lazy val scala212               = "2.12.20"
lazy val scala211               = "2.11.12"
lazy val supportedScalaVersions = List(scala211, scala212)

ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := scala212
ThisBuild / organization := "africa.absa.cps"

lazy val root = (project in file("."))
  .settings(
    name                 := "dataset-comparison",
    crossScalaVersions   := supportedScalaVersions,
    assembly / mainClass := Some("africa.absa.cps.DatasetComparison"),
    libraryDependencies ++= bigfilesDependencies ++ Seq(
      "org.apache.spark" %% "spark-core"     % sparkVersionForScala(scalaVersion.value) % Provided,
      "org.apache.spark" %% "spark-sql"      % sparkVersionForScala(scalaVersion.value) % Provided,
      "org.json4s"       %% "json4s-native"  % jsonVersionForScala(scalaVersion.value),
      "org.json4s"       %% "json4s-jackson" % jsonVersionForScala(scalaVersion.value),
      "org.apache.hadoop" % "hadoop-common"  % hadoopVersionForScala(scalaVersion.value),
      "org.apache.hadoop" % "hadoop-client"  % hadoopVersionForScala(scalaVersion.value),
      "org.apache.hadoop" % "hadoop-hdfs"    % hadoopVersionForScala(scalaVersion.value)
    ),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    Test / fork          := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value,
    packageOptions := Seq(
      ManifestAttributes(
        ("Built-By", System.getProperty("user.name")),
        ("Built-At", LocalDateTime.now().toString),
        ("Git-Hash", git.gitHeadCommit.value.getOrElse("unknown"))
      )
    )
  )

// JaCoCo code coverage
Test / jacocoReportSettings := JacocoReportSettings(
  title = s"{project} Jacoco Report - scala:${scalaVersion.value}",
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

Test / jacocoExcludes := Seq("africa.absa.cps.DatasetComparison*")

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
