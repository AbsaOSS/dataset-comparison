import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val spark3 = "3.5.3"

    val fansi = "0.4.0"
    val scalatest = "3.2.19"
    val scalastic = "3.0.0"
    val scalaMockito = "1.17.37"
    val scalaLangJava8Compat = "1.0.2"
  }

  def bigfilesDependencies: Seq[ModuleID] = {
    lazy val fansi = "com.lihaoyi" %% "fansi" % Versions.fansi
    lazy val sparkCore = "org.apache.spark" %% "spark-core" % Versions.spark3 % Provided
    lazy val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark3 % Provided

    lazy val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test

    Seq(
      scalatest,
      fansi,
      sparkCore,
      sparkSql
    )
  }
}
