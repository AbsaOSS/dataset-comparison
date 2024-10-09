import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val spark2 = "2.4.7"
    val spark3 = "3.5.3"

    val fansi = "0.4.0"
    val scalatest = "3.2.19"
    val scalastic = "3.0.0"
    val scalaMockito = "1.17.37"
    val scalaLangJava8Compat = "1.0.2"
  }

  def bigfilesDependencies: Seq[ModuleID] = {
    lazy val scalastic = "org.scalactic" %% "scalactic" % Versions.scalastic
    lazy val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test
    lazy val fansi = "com.lihaoyi" %% "fansi" % Versions.fansi
    lazy val sparkCore = "org.apache.spark" %% "spark-core" % Versions.spark3
    lazy val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark3

    Seq(
      scalastic,
      scalatest,
      fansi,
      sparkCore,
      sparkSql
    )
  }
}
