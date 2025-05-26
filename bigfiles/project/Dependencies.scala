/** Copyright 2020 ABSA Group Limited
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */

import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val spark3 = "3.5.3"
    val spark2 = "2.4.7"

    val jackson211_212 = "2.17.2"
    val json35         = "3.5.3"
    val json36         = "3.6.6"

    val fansi     = "0.4.0"
    val scalatest = "3.2.19"

    val scopt    = "4.1.0"
    val slf4jApi = "2.0.16"
    val logback  = "1.2.3"
    val config   = "1.4.3"
    val upickle3 = "3.3.1"
    val upickle1 = "1.4.0"

    val hadoop2 = "2.6.5"
    val hadoop3 = "3.3.5"
  }

  def sparkVersionForScala(scalaVersion: String): String = {
    scalaVersion match {
      case _ if scalaVersion.startsWith("2.11") => Versions.spark2
      case _ if scalaVersion.startsWith("2.12") => Versions.spark3
      case _ => throw new IllegalArgumentException("Only Scala 2.11 and 2.12 are currently supported.")
    }
  }

  def jsonVersionForScala(scalaVersion: String): String = {
    scalaVersion match {
      case _ if scalaVersion.startsWith("2.11") => Versions.json35
      case _ if scalaVersion.startsWith("2.12") => Versions.json36
      case _ => throw new IllegalArgumentException("Only Scala 2.11 and 2.12 are currently supported.")
    }
  }

  def hadoopVersionForScala(scalaVersion: String): String = {
    scalaVersion match {
      case _ if scalaVersion.startsWith("2.11") => Versions.hadoop2
      case _ if scalaVersion.startsWith("2.12") => Versions.hadoop3
      case _ => throw new IllegalArgumentException("Only Scala 2.11 and 2.12 are currently supported.")
    }
  }

  def unpickleVersionForScala(scalaVersion: String): String = {
    scalaVersion match {
      case _ if scalaVersion.startsWith("2.11") => Versions.upickle1
      case _ if scalaVersion.startsWith("2.12") => Versions.upickle3
      case _ => throw new IllegalArgumentException("Only Scala 2.11 and 2.12 are currently supported.")
    }
  }

  def bigfilesDependencies: Seq[ModuleID] = {
    lazy val fansi    = "com.lihaoyi"      %% "fansi"     % Versions.fansi
    lazy val scopt    = "com.github.scopt" %% "scopt"     % Versions.scopt
    lazy val slf4jApi = "org.slf4j"         % "slf4j-api" % Versions.slf4jApi exclude ("log4j", "log4j")
    lazy val config   = "com.typesafe"      % "config"    % Versions.config

    lazy val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatest % Test

    // Required for scala 2.11 + spark 2.4.7
    lazy val snappy  = "org.xerial.snappy"             % "snappy-java"          % "1.1.8.4"
    lazy val jackson = "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson211_212 % Provided

    Seq(
      scalatest,
      fansi,
      scopt,
      slf4jApi,
      config,
      jackson,
      snappy
    )
  }
}
