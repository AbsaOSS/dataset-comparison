package africa.absa.cps.writers

import africa.absa.cps.models.RunData
import org.apache.spark.sql.DataFrame

class AWSWriter extends Writer {
override def write(filePath: String, data: DataFrame): Unit = println(s"Writing $data to AWS") //todo: implement
}
