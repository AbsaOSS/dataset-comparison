import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val spark3 = "3.5.3"

    val fansi = "0.4.0"
    val scalatest = "3.2.19"
    val json = "3.6.6"
    val scopt = "4.1.0"
  }

  def bigfilesDependencies: Seq[ModuleID] = {
    lazy val fansi = "com.lihaoyi" %% "fansi" % Versions.fansi
    lazy val sparkCore = "org.apache.spark" %% "spark-core" % Versions.spark3 % Provided
    lazy val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark3 % Provided
    lazy val json = "org.json4s" %% "json4s-native" % Versions.json
    lazy val scopt = "com.github.scopt" %% "scopt" % Versions.scopt

    lazy val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test

    Seq(
      scalatest,
      fansi,
      sparkCore,
      sparkSql,
      json,
      scopt
    )
  }
}
