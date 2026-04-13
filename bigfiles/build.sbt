/*
 * Copyright 2024 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
ThisBuild / organization := "za.co.absa"

// ── api module ────────────────────────────────────────────────────────────────
// Pure comparison logic and data models. No CLI or I/O concerns.
lazy val api = (project in file("api"))
  .enablePlugins(JacocoFilterPlugin)
  .settings(
    name               := "dataset-comparison-api",
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= apiDependencies(scalaVersion.value),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    Test / fork          := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value / "api"
  )

// ── app module ────────────────────────────────────────────────────────────────
// CLI entry point, argument parsing, I/O and serialization. Depends on api.
lazy val app = (project in file("app"))
  .enablePlugins(JacocoFilterPlugin)
  .dependsOn(api)
  .settings(
    name                 := "dataset-comparison",
    crossScalaVersions   := supportedScalaVersions,
    assembly / mainClass := Some("za.co.absa.DatasetComparison"),
    libraryDependencies ++= appDependencies(scalaVersion.value),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    Test / fork          := true,
    Test / baseDirectory := (ThisBuild / baseDirectory).value / "app",
    packageOptions := Seq(
      ManifestAttributes(
        ("Built-By", System.getProperty("user.name")),
        ("Built-At", LocalDateTime.now().toString),
        ("Git-Hash", git.gitHeadCommit.value.getOrElse("unknown"))
      )
    )
  )

// ── root aggregate ────────────────────────────────────────────────────────────
lazy val root = (project in file("."))
  .enablePlugins(JacocoFilterPlugin)
  .enablePlugins(GitVersioning, GitBranchPrompt)
  .enablePlugins(ScalafmtPlugin)
  .aggregate(api, app)
  .settings(
    name := "dataset-comparison-root",
    // Prevent root from being published or assembled
    publish / skip := true
  )
