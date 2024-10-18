import Dependencies.*

ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "africa.absa.cps"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-comparison",
    assembly / mainClass := Some("africa.absa.cps.Main"),
    libraryDependencies ++= bigfilesDependencies,
    Test / fork := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value
  )

// JaCoCo code coverage
Test / jacocoReportSettings := JacocoReportSettings(
  title = s"{project} Jacoco Report - scala:${scalaVersion.value}",
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

