package x_part2dataframes

import org.apache.spark.sql.Row
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, _}

object DataFramesBasics {

  def main(args: Array[String]): Unit = {

    // creating a spark session
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrames Basics")
      .config("spark.master", "local")
      .getOrCreate()

    // reading a data frame from a file
    val firstDF: DataFrame = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")

    // printing the data frame (first 20 rows usually)
    firstDF.show()

    // printing the schema od DF
    firstDF.printSchema()

    // rows don't know schema, they are just tuples
    val rows: Array[Row] = firstDF.take(10)
    rows.foreach(println)

    // spark types
    val longType: LongType.type = LongType

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
    carsSchema.foreach(println)

    val carsDFSchema: StructType = firstDF.schema
    // carsDFSchema.foreach(println)

    val dataFrame2: DataFrame = spark.read
      .format("json")
      .schema(carsSchema)
      .load("src/main/resources/data/cars.json")
    dataFrame2.show()

    // create rows by hand
    val rows2: Seq[Row] = Seq(
      Row("aa", 23, "USA"),
      Row("bb", 24, "Canada"),
      Row("cc", 25, "Mexico"),
      Row("dd", 26, "Bolivia")
    )

    // creating a data frame out of tuples #1
    val tuples2: Seq[(String, Int, String)] = Seq(
      ("aa", 23, "USA"),
      ("bb", 24, "Canada"),
      ("cc", 25, "Mexico"),
      ("dd", 26, "Bolivia")
    )
    val frame2: DataFrame                   = spark.createDataFrame(tuples2)

    // creating a data frame out of tuples #2
    import spark.implicits._
    val frame3: DataFrame = tuples2.toDF("abbreviation", "index", "Country")

    /** Exercise 1
      *   - create a manual DF descibing smartphones and print its details to the console
      *   - read another file from resources - movies.json
      *     - print its schema
      *     - count the number of rows, by calling `count`
      */

    val smartPhoneTuples: Seq[(String, String, Float, Int)] = Seq(
      // manufacturer, model, screen size, camera MPixels
      ("Motorola", "X 10", 9.7f, 12),
      ("Xiaomi", "Book 12", 9.6f, 13),
      ("Redmi", "Note 9", 10.3f, 11),
      ("Google", "Pixel 3", 11.1f, 13)
    )

    val smartPhonesDF: DataFrame =
      smartPhoneTuples.toDF("Manufacturer", "Model", "Screen size", "Camera MegaPixels")
    smartPhonesDF.show()
    smartPhonesDF.printSchema()

    // reading movies #1
    val moviesDF: DataFrame = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/movies.json")
    moviesDF.show()
    moviesDF.printSchema()
    println(s"the count of movies: ${moviesDF.count()}")

    // reading movies #2
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

    val moviesDF2: DataFrame = spark.read
      .format("json")
      .schema(moviesSchema)
      .load("src/main/resources/data/movies.json")
    moviesDF2.show()
    moviesDF2.printSchema()
    println(s"the count of movies: ${moviesDF2.count()}")

  }

}
