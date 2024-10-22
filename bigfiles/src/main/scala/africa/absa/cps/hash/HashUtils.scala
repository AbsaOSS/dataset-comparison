package africa.absa.cps.hash

import africa.absa.cps.{Comparator, Main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, struct, udf}
import org.slf4j.{Logger, LoggerFactory}

object HashUtils {

  private val logger: Logger = LoggerFactory.getLogger(HashUtils.getClass)
  private val hashUDF = udf((row: Row) => hashRow(row))

  /**
   * Hash a row
   *
   * @param row Row to hash
   * @return Hash of the row
   */
  private def hashRow(row: Row): Int = row.mkString.hashCode

  /**
   * Hash the dataframe
   *
   * @param df DataFrame to hash
   * @return DataFrame with hash column
   */
  def createHashColumn(df: DataFrame): DataFrame = {
    logger.info("Hashing the dataframe")
    df.withColumn(Comparator.HashName , hashUDF(struct(df.columns.map(col): _*)))
  }

}
