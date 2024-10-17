package africa.absa.cps.hash

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, struct, udf}

object HashTable {
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
    val hashUDF = udf((row: Row) => hashRow(row))
    table.withColumn("hash", hashUDF(struct(table.columns.map(col): _*)))
  }

}
