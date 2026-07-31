package x_part6practical

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Test Deploy App")
      .getOrCreate()

    val moviesDF: DataFrame = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    import spark.implicits._
    val goodComediesDF: DataFrame = moviesDF
      .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .select($"Title", $"IMDB_Rating" as "Rating", $"Release_Date")
      .orderBy($"Rating".desc_nulls_last)

    goodComediesDF.show()

    goodComediesDF
      .write
      .mode(SaveMode.Overwrite)
      .save(args(1))

  }

}
