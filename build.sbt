import Dependencies.Versions.spark3
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.20"
ThisBuild / name := "CPS-Dataset-Comparison"
ThisBuild / organization := "africa.absa.cps"

lazy val root = (project in file("."))
  .settings(
    name := "foo",
    idePackagePrefix := Some("africa.absa.cps"),
    libraryDependencies ++= Seq(
      "com.lihaoyi" % "fansi_2.13" % "0.4.0",
      "org.scalactic" %% "scalactic" % "3.0.0",
      "org.scalatest" %% "scalatest" % "3.2.9" % Test,
      "org.apache.spark" %% "spark-core" % spark3
    ),
  )

