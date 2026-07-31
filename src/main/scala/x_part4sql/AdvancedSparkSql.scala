package x_part4sql

import org.apache.spark.sql.classic.{DataFrame, SparkSession}

object AdvancedSparkSql {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Advanced Spark SQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "src/main/resources/warehouse2")
      .getOrCreate()

    val carsDF: DataFrame = sparkSession.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")
    carsDF.createOrReplaceTempView("cars")

    val moviesDF: DataFrame = sparkSession.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")
    moviesDF.createOrReplaceTempView("movies")

    /*
    ANSI mode
     */



  }
}
