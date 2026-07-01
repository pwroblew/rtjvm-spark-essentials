package x_part3typesdatasets

import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ManagingNulls {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Complex Data Types")
    .master("local")
    .getOrCreate()

  private val moviesDF: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF
    .select(
      col("Title"),
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating"),
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Final_Rating")
    )

  moviesDF.select("*")
    .where(col("Rotten_Tomatoes_Rating").isNull)

  moviesDF
    .orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last)

  moviesDF.select("Title", "IMDB_rating").na.drop()

  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))

  moviesDF.na.fill(Map(
    "IMDB_Rating"            -> 0,
    "Rotten_Tomatoes_Rating" -> 0,
    "Director"               -> "unknown"
  ))

  moviesDF
    .selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10)",
      "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0)"
    )
    .show()

  def main(args: Array[String]): Unit = {}
}
