package par4sql

object SparkSql extends App
{
  val spark = SparkSession.builder()
    .appName("Spark SQL Pratice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    //don't use for Spark 3:
    //.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  //use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
    )
  //In that way, is possible use sql to query database;

  //Creates a folder (spark_warehouse) where stores the new databases
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  //We can run ANY SQL statement
  val databasesDF = spark.sql("show databases")

  databasesDF.show()
  //transfer tables from a DB to Spark tables
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

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach
  { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse)
    {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable("employees")
    }
  }
  //snappy.parquet generated

  transferTables(List
                 ("employees",
                  "departments",
                  "titles",
                  "dept_emp",
                  "salaries",
                  "dept_managers"
                  )
                 )

  //read DF from warehouse
  val employeesDF2 = spark.read.table("employees")

  /**
   * Exercises
   *
   * 1. Read the moviesDF and store it as a Spark table in the rtjvm database.
   * 2. Count how many employees we have in between Jan 1 200 and Jan 1 2001.
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
   * 4. Show the name of the best-paying department for employees hired in between those dates.
   */

  spark.sql
  (
    """
      |select * from employees e, dept_emp d where e.emp_no = d.emp_no
      |""".stripMargin
    )

  //1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  //2
  spark.sql
  (
    """
      |select count(*) from employees where hire_date > '1999-01-01' and '2000-01-01';
      |""".stripMargin
    ).show()

  //3
  spark.sql
  (
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where hire_date > '1999-01-01'
      |and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin
  ).show()

  //4
  spark.sql
  (
    """
      |select vg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01'
      |and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and de.emp_no = d.emp_no
      |group by de.dept_name
      |order by payments desc
      |limit 1
    |""".stripMargin
  ).show()
}
