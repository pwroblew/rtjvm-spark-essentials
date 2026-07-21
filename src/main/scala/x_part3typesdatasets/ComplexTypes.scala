package x_part3typesdatasets

import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{try_to_date, _}

object ComplexTypes {

  private val sparkSession: SparkSession = SparkSession.builder()
    .appName("Complex Data Types")
    .master("local")
    .getOrCreate()

  private val moviesDF: DataFrame = sparkSession.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // DATES

  moviesDF
    .select(
      col("Title"),
      col("Release_Date"),
      coalesce(
        try_to_date(col("Release_Date"), "dd-MMM-yy"),
        try_to_date(col("Release_Date"), "d-MMM-yy"),
        try_to_date(col("Release_Date"), "yyyy-MM-dd")
        // try_to_date(col("Release_Date"), "MMMMMMMM, yyyy")
      ).as("Real_Release_Date")
    )
    .where(col("Real_Release_Date").isNull)
    .show()

  private val stocksDF: DataFrame = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF
    .select(
      col("symbol"),
      try_to_date(col("date"), "MMM d yyyy").as("Real_Date"),
      col("price")
    )
    // .filter(col("Real_Date").isNull)
    .show()

  // STRUCTURES

  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(
      col("Title"),
      col("Profit").getField("US_Gross").as("US_Profit")
    )
    .show()

  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .show()

  // ARRAYS
  private val moviesWithWords: DataFrame = moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Title_Words"))

  moviesWithWords
    .select(
      col("Title"),
      expr("Title_Words[0]"),
      size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love").as("Valentine_Movie")
    )
    .where(col("Valentine_Movie"))
    .show()

  def main(args: Array[String]): Unit = {}
}
