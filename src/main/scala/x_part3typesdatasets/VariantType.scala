package x_part3typesdatasets

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object VariantType {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Variant Type")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val rawData: List[String] = List(
      """ {"name":"Alice", "age": 30, "address": {"city":"NYC", "zip":12345} } """,
      """ {"name":"Bob", "age": 25, "skills": ["Scala", "Apache Spark"] } """,
      """ {"name":"Charlie", "age": 40, "address": {"city":"SF"} } """
    )
    val jsonDF                = rawData.toDF("raw_json")

    val variantDF: DataFrame = jsonDF.select(parse_json(col("raw_json")).as("data"))

    variantDF.printSchema()
    variantDF.show(truncate = false)

    val extractedDF: DataFrame = variantDF.select(
      variant_get(col("data"), "$.name", "string").as("Name"),
      variant_get(col("data"), "$.age", "bigint").as("Age"),
      try_variant_get(col("data"), "$.address.city", "string").as("City")
    )

    extractedDF.show()

    val schemaDF: DataFrame = variantDF.select(schema_of_variant(col("data")))
    schemaDF.show(truncate = false)

    // ---------------------------------------------------------

    val moviesRaw: DataFrame         = sparkSession.read.text("src/main/resources/data/movies.json")
    val moviesJsonDF: DataFrame      = moviesRaw.select(parse_json(col("value")).as("data"))
    val best10moviesDS: Dataset[Row] = moviesJsonDF.select(
      variant_get(col("data"), "$.Title", "string").as("Title"),
      try_variant_get(col("data"), "$.IMDB_Rating", "string").as("IMDB_Rating")
    )
      .orderBy(col("IMDB_Rating").desc_nulls_last)
      .limit(10)
    best10moviesDS.show()

    val usDvdProfitDF: DataFrame = moviesJsonDF.select(
      variant_get(col("data"), "$.Title", "string").as("Title"),
      try_variant_get(col("data"), "$.US_DVD_Sales", "string").as("DVD_Profit")
    ).na.drop()
    usDvdProfitDF.show()
  }
}
