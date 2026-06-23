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

    // reading a data frame
    val firstDF: DataFrame = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")

    println("### printing the dataFrame - start")
    firstDF.show()
    println("### printing the dataFrame - end")

    println("### printing the schema from dataFrame - start")
    firstDF.printSchema()
    println("### printing the schema - end")

    val rows: Array[Row] = firstDF.take(10)
    // rows.foreach(println)

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
    // carsSchema.foreach(println)

    val carsDFSchema = firstDF.schema
    // carsDFSchema.foreach(println)

    val dataFrame2: DataFrame = spark.read
      .format("json")
      .schema(carsSchema)
      .load("src/main/resources/data/cars.json")
    dataFrame2.show()

    // create rows by hand
    val rows2 = Seq(
      Row("aa", 23, "USA"),
      Row("bb", 24, "Canada"),
      Row("cc", 25, "Mexico"),
      Row("dd", 26, "Bolivia")
    )

    val tuples2 = Seq(
      ("aa", 23, "USA"),
      ("bb", 24, "Canada"),
      ("cc", 25, "Mexico"),
      ("dd", 26, "Bolivia")
    )

    val frame2: DataFrame = spark.createDataFrame(tuples2)
    frame2.show()
    frame2.printSchema()

    import spark.implicits._
    val frame3: DataFrame = tuples2.toDF("abbreviation", "index", "Country")
    frame3.show()
    frame3.printSchema()

  }

}
