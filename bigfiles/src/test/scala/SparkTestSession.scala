/**
 * Copyright 2020 ABSA Group Limited
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
 **/

import org.apache.spark.sql.SparkSession

import java.io.File

object SparkTestSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("SparkTestSession")
    .config("spark.hadoop.fs.default.name", FS_URI)
    .config("spark.hadoop.fs.defaultFS", FS_URI)
    .master("local[*]")
    .getOrCreate()

  val FS_URI: String =  new File("src/test/resources").getAbsoluteFile.toURI.toString
}
