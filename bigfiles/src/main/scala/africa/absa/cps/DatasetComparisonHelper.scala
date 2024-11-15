package africa.absa.cps

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.slf4j.{Logger, LoggerFactory}

object DatasetComparisonHelper {

  private var logger: Logger = LoggerFactory.getLogger(DatasetComparisonHelper.getClass)

  /**
   * Exclude columns from a DataFrame
   *
   * @param df DataFrame to exclude columns from
   * @param toExclude columns to exclude
   * @param dfName name of the DataFrame
   * @param spark SparkSession
   * @return DataFrame with excluded columns
   */
  def exclude(df: DataFrame, toExclude: Seq[String], dfName: String)(implicit spark: SparkSession): DataFrame = {
    if (toExclude.isEmpty)  df
    else {
      logger.info(s"Excluding columns from the $dfName DataFrame")
      val toExcludeColumns = df.columns.filter(toExclude.contains) // get columns to exclude that are in the DataFrame
      toExclude.filterNot(df.columns.contains).foreach(col => logger.warn(s"Column $col not found in the $dfName DataFrame")) // log warning for columns that are not in the DataFrame
      df.drop(toExcludeColumns: _*)
    }
  }

}
