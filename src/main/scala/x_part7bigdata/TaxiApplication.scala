package x_part7bigdata

import cats.implicits.catsSyntaxSemigroup
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Encoder, Encoders, Row}
import org.knowm.xchart.{SwingWrapper, XYChartBuilder}

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import scala.reflect.ClassTag

case class PickUpTimestampDistance(pickUpTimestamp: java.sql.Timestamp, distance: Double)

object PeakHourAggregator
    extends Aggregator[(Timestamp, DistanceBucket), Map[Int, Long], Int] {
  override def zero: Map[Int, Long] = Map.empty

  override def reduce(
      acc: Map[Int, Long],
      el: (Timestamp, DistanceBucket)
  ): Map[Int, Long] = {
    val zonedDateTime: ZonedDateTime =
      ZonedDateTime.ofInstant(el._1.toInstant, ZoneId.of("America/New_York"))
    val hour: Int                    = zonedDateTime.getHour
    val current: Long                = acc.getOrElse(hour, 0)
    acc + ((hour, current + 1))
  }

  override def merge(b1: Map[Int, Long], b2: Map[Int, Long]): Map[Int, Long] = b1 |+| b2

  override def finish(reduction: Map[Int, Long]): Int = {
    reduction
      .toList
      .minBy(keyValue => keyValue._2 * -1)
      ._1
  }

  override def bufferEncoder: Encoder[Map[Int, Long]] = Encoders.kryo[Map[Int, Long]]

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

object TaxiApplication {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Taxi Big Data Application")
      .master("local[*]")
      .getOrCreate()

    val taxiDf: DataFrame = spark.read
      .load("src/main/resources/data/yellow_taxi_jan_25_2018")

    /** taxiDf.printSchema()
      *
      * root
      * |-- VendorID: integer (nullable = true)
      * |-- tpep_pickup_datetime: timestamp (nullable = true)
      * |-- tpep_dropoff_datetime: timestamp (nullable = true)
      * |-- passenger_count: integer (nullable = true)
      * |-- trip_distance: double (nullable = true)
      * |-- RatecodeID: integer (nullable = true)
      * |-- store_and_fwd_flag: string (nullable = true)
      * |-- PULocationID: integer (nullable = true)
      * |-- DOLocationID: integer (nullable = true)
      * |-- payment_type: integer (nullable = true)
      * |-- fare_amount: double (nullable = true)
      * |-- extra: double (nullable = true)
      * |-- mta_tax: double (nullable = true)
      * |-- tip_amount: double (nullable = true)
      * |-- tolls_amount: double (nullable = true)
      * |-- improvement_surcharge: double (nullable = true)
      * |-- total_amount: double (nullable = true)
      */

    /** taxiDf.show()
      *
      * | VendorID | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | RatecodeID | store_and_fwd_flag | PULocationID | DOLocationID | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount |
      * |:---------|:---------------------|:----------------------|:----------------|:--------------|:-----------|:-------------------|:-------------|:-------------|:-------------|:------------|:------|:--------|:-----------|:-------------|:----------------------|:-------------|
      * | 2        | 2018-01-25 00:02:56  | 2018-01-25 00:10:58   | 1               | 2.02          | 1          | N                  | 48           | 107          | 2            | 8.5         | 0.5   | 0.5     | 0.0        | 0.0          | 0.3                   | 9.8          |
      * | 2        | 2018-01-25 00:57:13  | 2018-01-25 01:21:17   | 1               | 10.13         | 1          | N                  | 79           | 244          | 2            | 28.5        | 0.5   | 0.5     | 0.0        | 0.0          | 0.3                   | 29.8         |
      * | 2        | 2018-01-25 02:29:32  | 2018-01-25 02:41:29   | 1               | 3.38          | 1          | N                  | 239          | 48           | 1            | 13.0        | 0.0   | 0.5     | 2.76       | 0.0          | 0.3                   | 16.56        |
      * | 2        | 2018-01-25 02:43:47  | 2018-01-25 03:10:50   | 1               | 8.6           | 1          | N                  | 48           | 36           | 1            | 28.0        | 0.0   | 0.5     | 7.2        | 0.0          | 0.3                   | 36.0         |
      * | 2        | 2018-01-25 03:36:35  | 2018-01-25 03:41:39   | 1               | 1.24          | 1          | N                  | 170          | 107          | 2            | 6.0         | 0.0   | 0.5     | 0.0        | 0.0          | 0.3                   | 6.8          |
      * | 2        | 2018-01-25 06:14:21  | 2018-01-25 06:30:04   | 2               | 2.47          | 1          | N                  | 151          | 238          | 1            | 12.5        | 0.0   | 0.5     | 2.66       | 0.0          | 0.3                   | 15.96        |
      * | 2        | 2018-01-25 06:31:54  | 2018-01-25 06:40:26   | 1               | 1.53          | 1          | N                  | 238          | 143          | 1            | 8.0         | 0.0   | 0.5     | 1.32       | 0.0          | 0.3                   | 10.12        |
      * | 2        | 2018-01-25 06:45:31  | 2018-01-25 06:59:40   | 2               | 1.9           | 1          | N                  | 239          | 237          | 1            | 11.0        | 0.0   | 0.5     | 2.36       | 0.0          | 0.3                   | 14.16        |
      * | 2        | 2018-01-25 07:05:28  | 2018-01-25 07:10:12   | 1               | 0.6           | 1          | N                  | 141          | 237          | 1            | 5.0         | 0.0   | 0.5     | 1.16       | 0.0          | 0.3                   | 6.96         |
      * | 2        | 2018-01-25 07:11:49  | 2018-01-25 07:17:53   | 1               | 0.66          | 1          | N                  | 236          | 236          | 1            | 5.5         | 0.0   | 0.5     | 1.26       | 0.0          | 0.3                   | 7.56         |
      */

    /** println(taxiDf.count()) 331893
      */

    val zonesDf: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/taxi_zones.csv")

    /** zonesDf.printSchema()
      *
      * root
      * |-- LocationID: integer (nullable = true)
      * |-- Borough: string (nullable = true)
      * |-- Zone: string (nullable = true)
      * |-- service_zone: string (nullable = true)
      */

    /** zonesDf.show()
      *
      * | LocationID |       Borough |                 Zone | service_zone |
      * |:-----------|--------------:|---------------------:|:-------------|
      * | 1          |           EWR |       Newark Airport | EWR          |
      * | 2          |        Queens |          Jamaica Bay | Boro Zone    |
      * | 3          |         Bronx | Allerton/Pelham G... | Boro Zone    |
      * | 4          |     Manhattan |        Alphabet City | Yellow Zone  |
      * | 5          | Staten Island |        Arden Heights | Boro Zone    |
      * | 6          | Staten Island | Arrochar/Fort Wad... | Boro Zone    |
      * | 7          |        Queens |              Astoria | Boro Zone    |
      * | 8          |        Queens |         Astoria Park | Boro Zone    |
      * | 9          |        Queens |           Auburndale | Boro Zone    |
      * | 10         |        Queens |         Baisley Park | Boro Zone    |
      * | 11         |      Brooklyn |           Bath Beach | Boro Zone    |
      * | 12         |     Manhattan |         Battery Park | Yellow Zone  |
      */

    /** println(zonesDf.count())
      *
      * 265
      */

    /** Questions:
      *
      *   1. Which zones have the most pickups/dropoffs overall?
      *   2. What are the peak hours for taxi?
      *   3. How are the trips distributed by length? Why are people taking the cab?
      *   4. What are the peak hours for long/short trips?
      *   5. What are the top 3 pickup/dropoff zones for long/short trips?
      *   6. How are people paying for the ride, on long/short trips?
      *   7. How is the payment type evolving with time?
      *   8. Can we explore a ride-sharing opportunity by grouping close short trips?
      */

    import spark.implicits._

    // 1. Which zones have the most pickups/dropoffs overall?

    def countByZones(): Unit = {

      val pickUpLocationIdCol: Column  = col("PULocationID")
      val dropOffLocationIdCol: Column = col("DOLocationID")

      def getCountBy(column2: Column): Dataset[Row] = {
        taxiDf
          .groupBy(column2)
          .agg(count("*").as("Count"))
          .join(zonesDf, column2 === col("LocationID"))
          .drop($"LocationID", $"service_zone")
          .orderBy(col("Count").desc_nulls_last)
      }

      val countByPickUpZone: Dataset[Row]  = getCountBy(pickUpLocationIdCol)
      val countByDropOffZone: Dataset[Row] = getCountBy(dropOffLocationIdCol)

      countByPickUpZone.show(truncate = false)
      countByDropOffZone.show(truncate = false)
    }
    // countByZones()

    // 2. What are the peak hours for taxi?

    def countByPickUpTime(): Unit = {
      val countByPickUpHour: Dataset[Row] =
        taxiDf.withColumn("Pick_Up_Hour", hour(col("tpep_pickup_datetime")))
          .groupBy(col("Pick_Up_Hour"))
          .agg(count("*").as("count"))
          .orderBy(col("Pick_Up_Hour"))

      val countByPickUpQuarter: Dataset[Row] = taxiDf.withColumn(
        "quarter",
        hour(col("tpep_pickup_datetime")) * 4 + floor(minute(col("tpep_pickup_datetime")) / 15)
      )
        .groupBy("quarter")
        .count()
        .orderBy(col("quarter"))

      // countByPickUpHour.show(25)
      // countByPickUpQuarter.show(100)

      val countByPickUpHourTyped: Dataset[(Long, Long)]    = countByPickUpHour.as[(Long, Long)]
      plotDataSet(countByPickUpHourTyped, "pick up hour", "count", "count by pick up hour")
      val countByPickUpQuarterTyped: Dataset[(Long, Long)] = countByPickUpQuarter.as[(Long, Long)]
      plotDataSet(countByPickUpQuarterTyped, "pick up quarter", "count", "count by pick up quarter")
    }
    // countByPickUpTime()

    // 3. How are the trips distributed by length? Why are people taking the cab?

    def countByTripDiscance() = {

      val countByDistance: Dataset[(DistanceBucket, Long)] = taxiDf
        .select($"trip_distance")
        .as[Double]
        .map(DistanceBucket.from)
        .groupByKey(identity)
        .count().as("count")
        .orderBy($"key".asc)

      countByDistance.show()
      plotDataSet(
        countByDistance.map(keyValue => ((keyValue._1.min * 10).toInt, keyValue._2)),
        "distance",
        "count",
        "count by distance"
      )

    }
    // countByTripDiscance()

    // 4. What are the peak hours for long/short trips?
    def peakHoursByDistance(): Unit = {

      val function: UserDefinedFunction = udaf(PeakHourAggregator)

      taxiDf
        .select(
          col("tpep_pickup_datetime").as("pickUpTimestamp"),
          col("trip_distance").as("distance")
        )
        .as[PickUpTimestampDistance]
        .map(pickUpTimestampDistance =>
          (
            pickUpTimestampDistance.pickUpTimestamp,
            DistanceBucket.from(pickUpTimestampDistance.distance)
          )
        )
        .groupByKey(pair => pair._2)
        .agg(PeakHourAggregator.toColumn)
        .map(elem => (elem._1.min, elem._2))
        .orderBy($"_1")
        .show()

    }
    peakHoursByDistance()

  }

  private def plotDataSet[T: ClassTag](
      values: Dataset[(T, Long)],
      xAxisTitle: String,
      yAxisTitle: String,
      title: String
  ): Unit = {
    val plotData: Array[(T, Long)] = values.collect()
    val x: List[T]                 = plotData.map(_._1).toList
    val y: List[Long]              = plotData.map(_._2).toList

    val chart = new XYChartBuilder()
      .width(800)
      .height(600)
      .title(title)
      .xAxisTitle(xAxisTitle)
      .yAxisTitle(yAxisTitle)
      .build()

    import scala.jdk.CollectionConverters._
    chart.addSeries("sum", x.asJava, y.map(java.lang.Long.valueOf).asJava)

    new SwingWrapper(chart).displayChart()
  }
}
