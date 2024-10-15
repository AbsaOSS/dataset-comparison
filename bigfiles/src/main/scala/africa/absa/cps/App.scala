//object Main{
//  def main(args: Array[String]): Unit = {
//
//    // Initialize Spark session
//    val spark = SparkSession.builder()
//      .appName("SimpleSparkApp")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // Sample data: sequence of tuples (name, age)
//    val data1 = List(
//      ("Alice", 29),
//      ("Bob", 31),
//      ("Cathy", 22),
//      ("David", 35)
//    )
//
//    val data2 = List(
//      ("Alice", 40),
//      ("Bob", 31),
//      ("Catherine", 22),
//      ("David", 35)
//    )
//
//    // Create DataFrames
//    val df1 = data1.toDF("name", "age")
//    val df2 = data2.toDF("name", "age")
//
//    // Show different rows
//    df1.exceptAll(df2).show()
//  }
//}
