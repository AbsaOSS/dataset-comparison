import Dependencies.*

ThisBuild / version := "1.0"
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
