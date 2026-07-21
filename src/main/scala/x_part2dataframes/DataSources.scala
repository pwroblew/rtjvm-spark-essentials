package x_part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataSources {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Data Sources and Formats")
      .config("spark.master", "local")
      .getOrCreate()

    // schema - first struct type
    val carsSchema = StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", IntegerType),
        StructField("Cylinders", IntegerType),
        StructField("Displacement", IntegerType),
        StructField("Horsepower", IntegerType),
        StructField("Weight_in_lbs", IntegerType),
        StructField("Acceleration", DoubleType),
        StructField("Origin", StringType),
        StructField("Year", StringType)
      )
    )

    val carsDF: DataFrame = spark.read
      .format("json")
      .schema(carsSchema)
      .option("mode", "failFast")
      .option("path", "src/main/resources/data/cars.json")
      .load()

    carsDF.show()
    carsDF.printSchema()

    val carsDF2: DataFrame = spark.read
      .options(Map(
        "inferSchema" -> "true",
        "mode"        -> "failFast",
        "path"        -> "src/main/resources/data/cars.json"
      ))
      .json()
    carsDF2.show()
    carsDF2.printSchema()

    carsDF2.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/cars2.json")

    // JSON flags
    println("******* JSON flags ********")

    val carsSchemaWithData     = StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", IntegerType),
        StructField("Cylinders", IntegerType),
        StructField("Displacement", IntegerType),
        StructField("Horsepower", IntegerType),
        StructField("Weight_in_lbs", IntegerType),
        StructField("Acceleration", DoubleType),
        StructField("Origin", StringType),
        StructField("Year", DateType)
      )
    )
    val carsJsonDF1: DataFrame = spark.read
      .schema(carsSchemaWithData)
      .options(Map(
        "dateFormat"        -> "yyyy-MM-dd",
        "allowSingleQuotes" -> "true",
        "compression"       -> "uncompressed" // bzip2, gzip, etc...
      ))
      .json("src/main/resources/data/cars.json")
    carsJsonDF1.show()
    carsJsonDF1.printSchema()

    // CSV

    val stockSchema = StructType(
      Array(
        StructField("symbol", StringType),
        StructField("date", DateType),
        StructField("price", DoubleType)
      )
    )

    val stocksDF: DataFrame = spark.read
      // .format("csv")
      .schema(stockSchema)
      .options(Map(
        "dateFormat" -> "MMM d yyyy",
        "header"     -> "true",
        "sep"        -> ",",
        "nullValue"  -> ""
      ))
      .csv("src/main/resources/data/stocks.csv")
    stocksDF.show()
    stocksDF.printSchema()

    // Parquet - default storing format for DataFrames!
    carsJsonDF1.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/cars.parquet")

    // text files
    spark.read.text("src/main/resources/data/simpleTextFile.txt").show()

    spark.read
      .format("jdbc")
      .options(Map(
        "driver"   -> "org.postgresql.Driver",
        "url"      -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user"     -> "docker",
        "password" -> "docker",
        "dbtable"  -> "public.employees"
      ))
      .load()
      .show()

    /** Exercises:
      *   1. read the movies DF from the file, and then write it as:
      *   - tab-separated csv
      *   - snappy Parquet
      *   - table in the postrgres DB - public.movies
      */

    val moviesSchema = StructType(
      Array(
        StructField("Title", StringType),
        StructField("US_Gross", IntegerType),
        StructField("Worldwide_Gross", IntegerType),
        StructField("US_DVD_Sales", LongType),
        StructField("Production_Budget", IntegerType),
        StructField("Release_Date", StringType),
        StructField("MPAA_Rating", StringType),
        StructField("Running_Time_min", LongType),
        StructField("Distributor", StringType),
        StructField("Source", StringType),
        StructField("Major_Genre", StringType),
        StructField("Creative_Type", StringType),
        StructField("Director", StringType),
        StructField("Rotten_Tomatoes_Rating", IntegerType),
        StructField("IMDB_Rating", FloatType),
        StructField("IMDB_Votes", IntegerType)
      )
    )

    val moviesDF: DataFrame = spark.read
      .format("json")
      .schema(moviesSchema)
      .load("src/main/resources/data/movies.json")

    moviesDF.write
      .format("csv")
      .options(Map(
        "sep" -> "\t"
      ))
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/movies.csv")

    moviesDF.write
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/movies.parquet")

    moviesDF.write
      .format("jdbc")
      .options(Map(
        "driver"   -> "org.postgresql.Driver",
        "url"      -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user"     -> "docker",
        "password" -> "docker",
        "dbtable"  -> "public.movies"
      ))
      .mode(SaveMode.Overwrite)
      .save()

  }

}
