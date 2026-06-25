package x_part2dataframes

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

object ColumnsAndExpressions {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("DF Columns and expressions")
      .config("spark.master", "local")
      .getOrCreate()

    val carsDF: DataFrame = spark.read
      .options(Map(
        "inferSchema" -> "true"
      ))
      .json("src/main/resources/data/cars.json")

    carsDF.show()

    // columns
    val nameCol: Column = carsDF.col("Name")

    // selecting (projecting => projections)
    val carNamesDF: DataFrame = carsDF.select(nameCol)
    carNamesDF.show()

    // various select methods
    val carsPartially_1: DataFrame = carsDF.select(
      carsDF.col("Name"),
      column("Acceleration")
    )
    carsPartially_1.show()

    import spark.implicits._
    val carsPartially_2: DataFrame = carsDF.select(
      carsDF.col("Name"),
      column("Acceleration"),
      $"Horsepower",
      expr("Origin")
    )
    carsPartially_2.show()

    val carsPartially_3: DataFrame = carsDF.select("Name", "Year")
    carsPartially_3.show()

    // EXPRESSIONS

    val colWeightInLbs: Column = carsDF.col("Weight_in_lbs")
    val colWeightInKgs: Column = colWeightInLbs / 2.2
    val carsP_4: DataFrame     = carsDF.select(carsDF.col("Name"), colWeightInLbs, colWeightInKgs)
    carsP_4.show()

    carsDF.select(
      column("Name"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kgs")
    ).show()

    carsDF.selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs / 2.2 as Weight_in_kg"
    ).show()

    // DF processing
    carsDF
      .withColumn("Weight_in_kgs_3", column("Weight_in_lbs") / 2.2)
      .withColumnRenamed("Weight_in_lbs", "Weight in pounds")
      .withColumnRenamed("Weight_in_kgs_3", "Weight in kgs")
      .selectExpr("Name", "`Weight in pounds`", "`Weight in kgs`")
      .show()

    carsDF
      .withColumn("Weight_in_kgs_3", column("Weight_in_lbs") / 2.2)
      .drop("Cylinders")
      .show()

    // filtering
    carsDF
      .filter(column("Origin") =!= "USA")
      .show()

    carsDF
      .filter("Origin = 'USA'")
      .show()

    val americalPowerfulCars_1: Dataset[Row] = carsDF
      .filter(column("Origin") === "USA")
      .filter(column("Horsepower") > 150)

    val americalPowerfulCars_2: Dataset[Row] = carsDF
      .filter(column("Origin") === "USA" and column("Horsepower") > 150)

    val americalPowerfulCars_3: Dataset[Row] = carsDF
      .filter("Origin = 'USA' and Horsepower > 150")
    americalPowerfulCars_3.show()

    val moreCarsDF: DataFrame = spark.read
      .options(Map(
        "inferSchema" -> "true"
      ))
      .json("src/main/resources/data/more_cars.json")

    val allCarsDF: Dataset[Row] = carsDF.union(moreCarsDF)

    val allCountries: Dataset[Row] = allCarsDF
      .select(column("Origin"))
      .distinct()
    allCountries.show()

    /** Exercises:
      *   1. read the movies DF and select 2 columns of my choice
      *   2. create another column - sum up the total profit of a movie: US + worldwide + dvd
      *   3. Select all COMEDIES with IMDB above 6
      *
      * for each exercise use as many ways/options, as possible
      */

    val moviesSchema      = StructType(
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
    val movies: DataFrame = spark.read
      .schema(moviesSchema)
      .json("src/main/resources/data/movies.json")

    movies
      .select(column("Title"), column("Director"))
      .show()

    movies
      .withColumn("Total_income", expr("US_Gross + Worldwide_Gross + US_DVD_Sales"))
      .select("Title", "Total_income", "Production_Budget")
      .withColumn("Profit", expr("Total_income - Production_Budget"))
      .show()

    movies
      .withColumn("Total_income", expr("US_Gross + Worldwide_Gross"))
      .select("Title", "Total_income", "Production_Budget")
      .withColumn("Profit", expr("Total_income - Production_Budget"))
      .show()

    movies
      .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
      .select("Title")
      .show()

  }
}
