package New_Ex.part2dataframes

import New_Ex.part2dataframes.DataSources.{driver, password, spark, url, user}
import Previews_exercises.part2dataframes.Joins.{bandsDF, guitaristsDF}

import scala.math

object Joins extends App
{
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  //inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandDF = guitaristsDF.join(bandsDF, guitaristsDF.col("band") === bandsDF.col("id"), "inner")
  guitaristsBandDF.show

  //outer joins
  //left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  //right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  //outer join = everything in the inner join + all the rows in the BOTH table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer").show()

  //semi-joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  //anti-joins = Missing rows in dataframe
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  //things to bear in mind
  //guitaristsBandDF.select("id", "band").show //this crashes

  //option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  //option 2 - drop the dupe column
  guitaristsBandDF.drop(bandsDF.col("id"))
  //Spark maintains unique identifiers

  //option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  //using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /*
  * Exercises
  * - Show all employees and theirs max salary
  * - Show all employees who were never managers
  * - Find the job titles of the best paid 10 employees in the company
  * */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtvim"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

  // 2
  val empNeverManagersDF = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),"left_anti")
  empNeverManagersDF.show()

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show
}
