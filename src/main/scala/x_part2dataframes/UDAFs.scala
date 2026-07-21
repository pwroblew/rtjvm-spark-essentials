package x_part2dataframes

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions._
import cats.syntax.option._

object UDAFs {

  case class CGRBuffer(maybePreviousValue: Option[Double], product: Double, nIntervals: Int)

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val carsDF = sparkSession.read
      .json("src/main/resources/data/cars.json")

    // we are concatenate all car names with a comma

    // 1. we need to definea scala function
    // and also need column type, buffer and the final type
    object Concatenator extends Aggregator[String, String, String] {
      override def zero: String                                    = ""
      override def reduce(buffer: String, value: String): String   =
        if (buffer.isEmpty) value
        else if (value.isEmpty) buffer
        else s"$buffer, $value"
      override def merge(buffer1: String, buffer2: String): String =
        if (buffer1.isEmpty) buffer2
        else if (buffer2.isEmpty) buffer1
        else s"$buffer1, $buffer2"
      override def finish(finalBuffer: String): String             = finalBuffer
      override def bufferEncoder: Encoder[String]                  = Encoders.STRING
      override def outputEncoder: Encoder[String]                  = Encoders.STRING
    }

    import sparkSession.implicits._

    object Concatenator2 extends Aggregator[String, List[String], String] {
      override def zero: List[String] = List.empty

      override def reduce(b: List[String], a: String): List[String] = a :: b

      override def merge(b1: List[String], b2: List[String]): List[String] = b1 ::: b2

      override def finish(reduction: List[String]): String =
        reduction.reverse.mkString("< ", " |#| ", " >")

      override def bufferEncoder: Encoder[List[String]] = implicitly[Encoder[List[String]]]

      override def outputEncoder: Encoder[String] = Encoders.STRING
    }

    // 2. register the above as a UDAF
    val concatenatorUDAF = udaf(Concatenator)

    // 3. apply it
    val allCarNamesDF = carsDF.select(concatenatorUDAF(col("Name")).as("All_Cars"))

    allCarNamesDF.show()
    allCarNamesDF.write
      .mode(SaveMode.Overwrite)
      .json("src/main/resources/data/cars-names.json")

    val concaternator2: UserDefinedFunction = udaf(Concatenator2)
    carsDF.select(concaternator2(col("Name")).as("ALL_NAMES")).write
      .mode(SaveMode.Overwrite)
      .json("src/main/resources/data/cars-names2.json")

    // lets evaluate CGR (Compound Growth Rate)

    object CGR extends Aggregator[Double, CGRBuffer, Double] {
      override def zero: CGRBuffer = CGRBuffer(None, 1.0, -1)

      override def reduce(oldBuffer: CGRBuffer, value: Double): CGRBuffer = {

        val newRatio: Double = oldBuffer.maybePreviousValue
          .map(previousValue => value / previousValue)
          .getOrElse(1.0)

        CGRBuffer(
          value.some,
          oldBuffer.product * newRatio,
          oldBuffer.nIntervals + 1
        )

      }

      override def merge(b1: CGRBuffer, b2: CGRBuffer): CGRBuffer =
        CGRBuffer(
          b2.maybePreviousValue.orElse(b1.maybePreviousValue),
          b1.product * b2.product,
          b1.nIntervals + b2.nIntervals + 1
        )

      override def finish(reduction: CGRBuffer): Double =
        math.pow(reduction.product, 1.0 / reduction.nIntervals)

      override def bufferEncoder: Encoder[CGRBuffer] = Encoders.product

      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }
    val udafCGR: UserDefinedFunction = udaf(CGR)

    val stocksDF: DataFrame = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/data/stocks.csv")

    stocksDF
      .groupBy(col("symbol"))
      .agg(
        udafCGR(col("price"))
      )
      .show()
  }
}
