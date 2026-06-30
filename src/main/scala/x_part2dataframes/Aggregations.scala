package x_part2dataframes

import org.apache.spark.sql.classic.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{sum, _}

object Aggregations {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Aggregations and grouping")
      .config("spark.master", "local")
      .getOrCreate()

    val moviesDF: DataFrame = sparkSession.read
      .options(Map(
        "inferSchema" -> "true"
      ))
      .json("src/main/resources/data/movies.json")

    // moviesDF.show()

    import sparkSession.implicits._
    moviesDF.select(count("*")).show()

    moviesDF.select(count(column("Major_Genre"))).show()
    moviesDF.select(countDistinct(column("Major_Genre"))).show()
    moviesDF.select(approx_count_distinct(column("Major_Genre"))).show()

    // stats
    moviesDF.select(min(column("IMDB_Rating")), max(column("IMDB_Rating"))).show()
    moviesDF.select(sum(column("US_Gross"))).show()

    moviesDF.select(avg(column("Rotten_Tomatoes_Rating"))).show()
    moviesDF.select(
      mean(column("Rotten_Tomatoes_Rating")),
      stddev(column("Rotten_Tomatoes_Rating"))
    ).show()

    // grouping

    moviesDF.groupBy("Major_Genre").count().show()

    moviesDF.groupBy("Major_Genre").avg("IMDB_Rating").show()

    moviesDF.groupBy(column("Major_Genre")).agg(
      count("*").as("N_movies"),
      avg("IMDB_Rating").as("Avg_rating")
    )
      .orderBy(col("Avg_rating"))
      .show()

    /** Excersises
      *   1. sum up all the profits of all the movies in the DF
      *   2. count how many distinct directors we have
      *   3. show the mean and stddev of us gross revenue for the movies
      *   4. compute the average imdb rating and avg us gross revenue per director
      */

    moviesDF.select(
      sum(column("US_Gross")),
      sum(column("Worldwide_Gross")),
      sum(column("US_DVD_Sales"))
    )
      .show()

    moviesDF.select(count(column("Director"))).show()
    moviesDF.select(countDistinct(column("Director"))).show()

    moviesDF.select(
      mean(column("US_Gross")),
      stddev(column("US_Gross"))
    )
      .show()

    moviesDF.groupBy(column("Director")).agg(
      avg(column("IMDB_Rating")),
      avg(column("US_Gross"))
    ).sort(column("avg(US_Gross)").desc_nulls_last)
      .show()

  }
}
