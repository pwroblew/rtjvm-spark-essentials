package x_part3typesdatasets

import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  Encoder,
  Encoders,
  KeyValueGroupedDataset,
  SparkSession
}
import org.apache.spark.sql.functions._

import java.time.LocalDate

object Datasets {

  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: LocalDate,
      Origin: String
  )

  case class Band(
      id: Long,
      name: String,
      hometown: String,
      year: String
  )

  case class Guitar(
      id: Long,
      model: String,
      make: String,
      guitarType: String
  )

  case class GuitarPlayer(
      id: Long,
      name: String,
      guitars: Seq[Long],
      band: Long
  )

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Datasets")
      .master("local[*]")
      .getOrCreate()

    val numbersDF: DataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/data/numbers.csv")

    numbersDF.printSchema()
    numbersDF
      .filter(col("numbers") < 100)
      .show()

    implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
    val numbersDS: Dataset[Int]           = numbersDF.as[Int]
    numbersDS.printSchema()
    numbersDS.show()

    val numbersDSFiltered: Dataset[Int] = numbersDS.filter(_ < 100)
    numbersDSFiltered.printSchema()
    numbersDSFiltered.show()

    // dataset of a complex class

    def readDF(fileName: String): DataFrame = {
      sparkSession.read
        .option("inferSchema", "true")
        .json(s"src/main/resources/data/$fileName")
    }

    val carsDF: DataFrame          = readDF("cars.json")
    val carsWithDatesDF: DataFrame =
      carsDF.withColumn("Year", try_to_date(col("Year"), "yyyy-MM-dd"))
    // implicit val carEncoder = Encoders.product[Car]
    import sparkSession.implicits._
    val carsDS: Dataset[Car]       = carsWithDatesDF.as[Car]

    carsDS
      .map(_.Name.toUpperCase())
      .explain(true)

    carsDS
      .map(_.Horsepower)
      .explain(true)

    carsDS
      .map(_.Horsepower)
      .filter(hp => hp.exists(_ > 140))
      .explain(true)

    // DS - a collection
    val carNamesDS: Dataset[String] = carsDS.map(_.Name.toUpperCase())

    println(s"we have ${carsDS.count} cars.")

    val powerfulCarsNum: Long = carsDS.filter(_.Horsepower.exists(hp => hp > 140)).count()
    println(s"we have $powerfulCarsNum powerful cars.")

    val horsePowersDS: Dataset[Long] = carsDS
      .filter(_.Horsepower.nonEmpty)
      .map(_.Horsepower.getOrElse(-1L))

    val frame: DataFrame = horsePowersDS.select(avg(col("value")))
    frame.show()

    ///////////////////////////////////

    val bandsDS: Dataset[Band]            = readDF("bands.json").as[Band]
    val guitars: Dataset[Guitar]          = readDF("guitars.json").as[Guitar]
    val guitarists: Dataset[GuitarPlayer] = readDF("guitarPlayers.json").as[GuitarPlayer]

    bandsDS.show()
    guitars.show()
    guitarists.show()

    // JOINS
    val guitaristsBandsDS: Dataset[(GuitarPlayer, Band)] =
      guitarists.joinWith(bandsDS, guitarists.col("band") === bandsDS.col("id"))
    guitaristsBandsDS.show()

    /*
    Exercise
    joing guitarists with guitars on the considiont that array_contains...
    use outer_join
     */

    val guitarsWithPlayers: Dataset[(GuitarPlayer, Guitar)] =
      guitarists.joinWith(
        guitars,
        array_contains(guitarists.col("guitars"), guitars.col("id")),
        "outer"
      )

    guitarsWithPlayers.show()

    // GROUPING
    val carsGroupedByOrigin: KeyValueGroupedDataset[String, Car] = carsDS
      .groupByKey(_.Origin)
    carsGroupedByOrigin
      .count()
      .show()

  }
}
