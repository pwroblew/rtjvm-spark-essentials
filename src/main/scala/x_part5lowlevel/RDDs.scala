package x_part5lowlevel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, Row, SaveMode}
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}

import scala.io.Source

object RDDs {

  case class Movie(title: String, genre: Option[String], rating: Option[Double])

  case class StockValue(symbol: String, date: String, price: Double)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Introduction to RDDs")
      .config("spark.master", "local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val numbers: Seq[Int]   = 1 to 1000000
    val numberRdd: RDD[Int] = sc.parallelize(numbers)

    // reading

    def readStocks(filename: String): List[StockValue] = Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

    val stocksRdd: RDD[StockValue] =
      sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

    val stocksRdd2: RDD[StockValue] = sc.textFile("src/main/resources/data/stocks.csv")
      .map(line => line.split(","))
      .filter(tokens => tokens(0).toUpperCase == tokens(0))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

    val stocksDf: DataFrame = spark.read
      .option("header", "true")
      .option("separator", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/stocks.csv")

    import spark.implicits._
    val stocksDS: Dataset[StockValue] = stocksDf.as[StockValue]
    val stocksRdd3: RDD[StockValue]   = stocksDS.rdd
    val rowsRdd: RDD[Row]             = stocksDf.rdd

    val value: Dataset[StockValue] = stocksRdd3.toDS()
    val frame: DataFrame           = stocksRdd3.toDF("symbol", "date", "price")

    // --------------------------------------------------------

    // transformations
    val msftRdd         = stocksRdd2.filter(_.symbol == "MSFT") // transformation - lazy
    val msftCount: Long = msftRdd.count()                       // action - eager

    val companies: RDD[String] = stocksRdd.map(_.symbol).distinct() // lazy

    implicit val stockOrdering: Ordering[StockValue] =
      Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
    val lowestMsft: StockValue                       = msftRdd.min() // action

    // reduce
    val sumNumber: Int = numberRdd.reduce(_ + _)

    // grouping
    val groupedStocks: RDD[(String, Iterable[StockValue])] = stocksRdd
      .groupBy(_.symbol)
    // ^^^^^ (VERY EXPENSIVE)

    // partitioning
    val repartitionedStocksRdd: RDD[StockValue] = stocksRdd3.repartition(30)
    repartitionedStocksRdd.toDF.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/stocks30")
    // ^^^^ expensive

    // coalescing -> less partitions
    val coalescedRDD: RDD[StockValue] =
      repartitionedStocksRdd.coalesce(15) // doesn't mean FULL shuffling

    /*
    Exercises
    1. read the movies.json as RDD
    2. show the discint genres as RDD
    3. select all the movies in the Drama genre, with IMDB rating > 6
    4. show the average rating of movies by genre
     */

    import spark.implicits._
    // implicit def movieEncoder: Encoder[Movie] = Encoders.product
    // 1.
    val moviesRdd: RDD[Movie] = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")
      .select($"Title", $"Major_Genre".as("Genre"), $"IMDB_Rating".as("Rating"))
      .as[Movie]
      .rdd
    // moviesRdd.take(5).foreach(println)

    val genresRdd: RDD[String] = moviesRdd
      .map(_.genre.fold("")(identity))
      .filter(_.nonEmpty)
      .distinct()

    // genresRdd.foreach(println)

    val goodDramasRdd: RDD[Movie] = moviesRdd
      .filter(_.genre.contains("Drama"))
      .filter(_.rating.exists(_ > 6.0d))

    // goodDramasRdd.foreach(println)

    val genreAvgRatingsRdd: RDD[(String, Double)] = moviesRdd
      .map(movie =>
        (movie.genre.fold("")(identity), movie.rating.fold(0.0d)(identity))
      )
      .groupBy(data => data._1)
      .map(genreInfo =>
        (
          genreInfo._1,
          genreInfo._2.foldLeft[(Double, Int)]((0.0d, 0))((acc, next) =>
            (acc._1 + next._2, acc._2 + 1)
          )
        )
      )
      .map(elem =>
        if (elem._2._2 == 0) (elem._1, 0)
        else (elem._1, elem._2._1 / elem._2._2)
      )
    genreAvgRatingsRdd.toDF.show

  }
}
