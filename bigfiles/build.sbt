import Dependencies.*
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.20"
ThisBuild / name := "CPS-Dataset-Comparison"
ThisBuild / organization := "africa.absa.cps"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-comparison",
    idePackagePrefix := Some("africa.absa.cps"),
    libraryDependencies ++= bigfilesDependencies
  )
  .enablePlugins(AssemblyPlugin)
