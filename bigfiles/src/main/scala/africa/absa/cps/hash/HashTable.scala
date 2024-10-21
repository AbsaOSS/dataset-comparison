package africa.absa.cps.hash

import africa.absa.cps.Main
import africa.absa.cps.writers.SparkWriter.logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, struct, udf}
import org.slf4j.{Logger, LoggerFactory}

object HashTable {

  private val logger: Logger = LoggerFactory.getLogger(HashTable.getClass)

  /**
   * Hash a row
   *
   * @param row Row to hash
   * @return Hash of the row
   */
  private def hashRow(row: Row): Int = row.mkString.hashCode

  /**
   * Hash the table
   *
   * @param table DataFrame to hash
   * @return DataFrame with hash column
   */
  def hash(table: DataFrame): DataFrame = {
    logger.info("Hashing the table")
    val hashUDF = udf((row: Row) => hashRow(row))
    table.withColumn(Main.HashName , hashUDF(struct(table.columns.map(col): _*)))
  }

}
