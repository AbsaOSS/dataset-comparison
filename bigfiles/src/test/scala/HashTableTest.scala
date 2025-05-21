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

import africa.absa.cps.Comparator
import africa.absa.cps.hash.HashUtils
import africa.absa.cps.hash.HashUtils.HASH_COLUMN_NAME
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class HashTableTest extends AnyFunSuite{
  val spark: SparkSession = SparkTestSession.spark

  import spark.implicits._

  test("test that hash function always get same results for exactly the same input") {
    val df: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val hash1 = HashUtils.createHashColumn(df)
    val hash2 = HashUtils.createHashColumn(df)
    assert(hash1.select(HASH_COLUMN_NAME).head() == hash2.select(HASH_COLUMN_NAME).head())
    assert(hash1.select(HASH_COLUMN_NAME).head(2)(1) == hash2.select(HASH_COLUMN_NAME).head(2)(1))
  }
  test("test that hash function always get same results for  same input") {
    val df1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val df2: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val hash1 = HashUtils.createHashColumn(df1)
    val hash2 = HashUtils.createHashColumn(df2)
    assert(hash1.select(HASH_COLUMN_NAME).head() == hash2.select(HASH_COLUMN_NAME).head())
    assert(hash1.select(HASH_COLUMN_NAME).head(2)(1) == hash2.select(HASH_COLUMN_NAME).head(2)(1))
  }
  test("test that hash function always get different results for different input") {
    val df1: DataFrame = Seq((1, "one"), (2, "two")).toDF("id", "value")
    val df2: DataFrame = Seq((3, "three"), (4, "four")).toDF("id", "value")
    val hash1 = HashUtils.createHashColumn(df1)
    val hash2 = HashUtils.createHashColumn(df2)
    assert(hash1.select(HASH_COLUMN_NAME).head() != hash2.select(HASH_COLUMN_NAME).head())
    assert(hash1.select(HASH_COLUMN_NAME).head(2)(1) != hash2.select(HASH_COLUMN_NAME).head(2)(1))
  }
}
