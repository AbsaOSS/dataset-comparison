import Dependencies.*

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.20"
ThisBuild / organization := "africa.absa.cps"

lazy val root = (project in file("."))
  .settings(
    name := "dataset-comparison",
    libraryDependencies ++= bigfilesDependencies
  )
