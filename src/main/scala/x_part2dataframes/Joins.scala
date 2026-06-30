package x_part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Joins {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Data Sources and Formats")
      .config("spark.master", "local")
      .getOrCreate()

    val guitarsDF: DataFrame = spark.read
      .options(Map(
        "inferSchema" -> "true"
      ))
      .json("src/main/resources/data/guitars.json")

    val guitaristsDF: DataFrame = spark.read
      .options(Map(
        "inferSchema" -> "true"
      ))
      .json("src/main/resources/data/guitarPlayers.json")

    val bandsDF: DataFrame = spark.read
      .options(Map(
        "inferSchema" -> "true"
      ))
      .json("src/main/resources/data/bands.json")

//    guitarsDF.show()
//    guitaristsDF.show()
//    bandsDF.show()

    val joinCondition: Column      = guitaristsDF.col("band") === bandsDF.col("id")
    val guitaristsBands: DataFrame =
      guitaristsDF.join(bandsDF, joinCondition, "inner") // inner is default
    guitaristsBands.show()

    guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()
    guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()
    guitaristsDF.join(bandsDF, joinCondition, "outer").show()

    // semi-joins
    guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()
    // anti-join
    guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

    /*
    Exercises
    1. show all employees and their max salary
    2. show all employees who were never managers
    3. find th job titles of the best paid 10 employees
     */

    def loadTable(tableName: String): DataFrame = {
      spark.read
        .format("jdbc")
        .options(Map(
          "driver"   -> "org.postgresql.Driver",
          "url"      -> "jdbc:postgresql://localhost:5432/rtjvm",
          "user"     -> "docker",
          "password" -> "docker",
          "dbtable"  -> ("public." + tableName)
        ))
        .load()
    }

    val employeesDF: DataFrame    = loadTable("employees")
    val salariesDF: DataFrame     = loadTable("salaries")
    val deptManagersDF: DataFrame = loadTable("dept_manager")
    val titlesDF: DataFrame       = loadTable("titles")

    val maxSalaries: DataFrame = salariesDF
      .groupBy(salariesDF.col("emp_no"))
      .agg(
        max("salary").as("max_salary")
      )
    val ex1_DF: DataFrame      = employeesDF
      .join(maxSalaries, employeesDF.col("emp_no") === maxSalaries.col("emp_no"))
      .drop(maxSalaries.col("emp_no"))
    ex1_DF.show()

    val neverManagersDF: DataFrame = employeesDF
      .join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"), "left_anti")
    neverManagersDF.show()

    val maxSalaries2: DataFrame     = maxSalaries.withColumnRenamed("max_salary", "salary")
    val maxSalariesFinal: DataFrame = salariesDF.join(maxSalaries2, List("emp_no", "salary"))

    val result: DataFrame = maxSalariesFinal
      .sort(column("salary").desc_nulls_last)
      .limit(10)
      .join(titlesDF, List("emp_no"))
//      .select(salariesDF.col("emp_no"), maxSalaries2.col("salary"), titlesDF.col("title"))
    result.show()

  }

}
