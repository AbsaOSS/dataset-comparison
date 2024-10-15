package africa.absa.cps.hash

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, struct, udf}

object HashTable {
  private def hashRow(row: Row): Int = {
    row.mkString.hashCode //todo implement better hash function
  }
  def hash(table: DataFrame): DataFrame = {
    val hashUDF = udf((row: Row) => hashRow(row))
    table.withColumn("hash", hashUDF(struct(table.columns.map(col): _*)))
  }

}
