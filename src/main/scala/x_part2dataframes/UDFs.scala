package x_part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFs {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .getOrCreate()

    val moviesDF = sparkSession.read
      .json("src/main/resources/data/movies.json")

    val countrWords   = (text: String) => text.split("\\s+").length
    val countWordsUDF = udf(countrWords)

    moviesDF.select(
      col("Title"),
      countWordsUDF(col("Title").as("Title_Words"))
    )
      .show()

    // exercise

    val carsDF = sparkSession.read
      .json("src/main/resources/data/cars.json")

    val getBrand    =
      (name: String) => name.split("\\s+").headOption.map(_.toUpperCase).getOrElse("[NONE]")
    val getBrandUDF = udf(getBrand)
    carsDF
      .select(getBrandUDF(col("Name")))
      .distinct()
      .show()

  }
}
