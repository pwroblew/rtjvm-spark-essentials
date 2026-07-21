package x_part3typesdatasets

import org.apache.spark.sql.Column
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CommonTypes {

  val sparkSession = SparkSession.builder()
    .appName("Common Spark Types")
    .master("local")
    .getOrCreate()

  private val moviesDF: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()
  moviesDF
    .withColumn("plain_value", expr("42"))
    .select(col("Title"), col("plain_value"))
    .show()

  // BOOLEANS
  private val moviesDramaFilter: Column = col("Major_Genre") === "Drama"
  private val goodRatingFilter: Column  = col("IMDB_Rating") > 7.0
  private val combinedFilter            = moviesDramaFilter and goodRatingFilter
  moviesDF
    .select(col("Title"))
    .where(moviesDramaFilter)

  val moviesWithGoodnessFlagsDF: DataFrame =
    moviesDF.select(col("Title"), combinedFilter.as("good_movie"))

  moviesWithGoodnessFlagsDF.where("good_movie")

  // NUMBERS

  moviesDF
    .select(
      col("Title"),
      (col("Rotten_Tomatoes_Rating") / 20.0 + col("IMDB_Rating") / 2.0).as("Avg_Rating")
    )

  // correlation
  private val ratingCorrelation: Double =
    moviesDF.stat.corr("IMDB_Rating", "Rotten_Tomatoes_Rating")
  println(s"Correlation rating: $ratingCorrelation")

  // STRINGS

  private val carsDF: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF
    .select(initcap(col("Name")))

  carsDF.select("*")
    .where(col("Name").contains("volkswagen"))

  // regexs
  val regexString = "volkswagen|vw"
  val vwDF        = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")

  vwDF.select(
    col("Name"),
    col("regex_extract"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /* Exercise
  1. filter the carsDF by a list of car names. The list is not known upfront. There is a method when called returns a list of strings.
   */

  def getCarNames: List[String] = List("toyota", "Honda")

  val regexCarNames = getCarNames.map(_.toLowerCase).mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexCarNames, 0).as("car_name_accepted")
  )
    .where(col("car_name_accepted") =!= "")
    .show()

  def main(args: Array[String]): Unit = {}
}
