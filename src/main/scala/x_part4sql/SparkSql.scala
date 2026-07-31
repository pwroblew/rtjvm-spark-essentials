package x_part4sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark SQL Practice")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
      // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .getOrCreate()

    val carsDF: DataFrame = sparkSession.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")

    val americanCarsDF: DataFrame = carsDF.select(col("Name")).where(col("Origin") === "USA")
    // americanCarsDF.show()

    // using Spark SQL

    // **** IMPORTANT ******
    carsDF.createOrReplaceTempView("cars") // cars df is now seen as a table!
    val americanCarsDF_2: DataFrame = sparkSession.sql(
      """
        |select Name from cars where Origin = "USA"
        |""".stripMargin
    )
    americanCarsDF_2.show()

    sparkSession.sql("create database rtjvm")
    sparkSession.sql("use rtjvm")
    val databasesDF: DataFrame = sparkSession.sql("show databases")
    databasesDF.show()

    // copying tables from a real DB to spark MANAGED tbles

    def readTable(tableName: String): DataFrame = {
      sparkSession.read
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

    val dbTables: List[String] = List(
      "departments",
      "dept_emp",
      "dept_manager",
      "employees",
      "movies",
      "salaries",
      "titles"
    )

    def transferTables(tablesNames: List[String]): Unit = tablesNames.foreach { tableName =>
      readTable(tableName)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }

    // transferTables(dbTables)

    //  sparkSession.sql("use rtjvm")
    //  private val tablesDF: DataFrame = sparkSession.sql("show tables")
    //  tablesDF.show()

    //  private val employeesDF3: DataFrame = sparkSession.read.table("employees")
    //  employeesDF3.show()

    /*
    1. read movies DF and store in spark table in rtjvm DB
    2. count how any employees were hired between Jan 1 2000 and Jan 1 2001
    3. show the average salaries for the employees hired between those dates, grouped by dept.
    4. show the name of the best paying dept for employees hired between thise dates.
     */

    // 1.
    val moviesDF: DataFrame = sparkSession.read
      .option("inferSchema", true)
      .json("src/main/resources/data/movies.json")

    //  moviesDF.write
    //    .mode(SaveMode.Overwrite)
    //    .saveAsTable("movies")

    // 2.
    val employeesDF: DataFrame = readTable("employees")
    employeesDF.createOrReplaceTempView("employees")

    val result2: DataFrame = sparkSession.sql(
      """
        | select count(*) from employees where hire_date > DATE '1999-01-01' AND hire_date < DATE '2000-01-01'
        |""".stripMargin
    )
    result2.show()

    // 3.

    val salariesDF: DataFrame    = readTable("salaries")
    salariesDF.createOrReplaceTempView("salaries")
    val dept_empDF: DataFrame    = readTable("dept_emp")
    dept_empDF.createOrReplaceTempView("dept_emp")
    val departmentsDF: DataFrame = readTable("departments")
    departmentsDF.createOrReplaceTempView("departments")

    val result3: DataFrame = sparkSession.sql(
      """
        |select avg(s.salary) AS avg_salary, d.dept_no, dd.dept_name
        |   FROM employees AS e
        |   JOIN salaries AS s
        |   ON e.emp_no = s.emp_no
        |   JOIN dept_emp as d
        |   ON e.emp_no = d.emp_no
        |   JOIN departments as dd
        |   on d.dept_no = dd.dept_no
        |   where e.hire_date >= DATE '1999-01-01' AND e.hire_date < DATE '2000-01-01' AND s.from_date = d.from_date
        |   group by d.dept_no, dd.dept_name
        |   order by avg_salary desc
        |""".stripMargin
    )
    result3.show()
  }
}
