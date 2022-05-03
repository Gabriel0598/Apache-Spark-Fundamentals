package New_Ex.part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}

object ColumnsAndExpressions extends App
{
  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  //Columns
  val firstColumn = carsDF.col("Name")

  //Selecting (projecting)
  val carNamesDF = carsDF(firstColumn)
  carNamesDF.show()

  //various select methods
  carsDF.select
  (
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, //Scala Symbol, auto-converted to column
    $"Horsepower", //fancier interpolated string, returns a Column object
    expr("Origin") //EXPRESSION
  )

  //select with plain column names
  carsDF.select("Name", "Year")

  //EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeightsDF = carsDF.select
  (
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  carsWithWeightsDF.show()

  //selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr
  (
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  //renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  //careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  //remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  //filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.filter(col("Origin") =!= "USA")
  //filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  //chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower")

  //unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) //works if the DFs have the same schema

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  /*
  * Exercises
  * 1. Read the movies DF
  * 2. Create another column summing up the total profit of the movies = US_Gross + worldwide
  * 3. Select all COMEDY movies with IMDB rating above 6
  *
  * Use as many versions as possible
  * */

  //1
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()

  val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesReleaseDF2 = moviesDF.select
  (moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating"))
  val moviesReleaseDF3 = moviesDF.selectExpr
  (
    "Title", "Release_Date"
  )

  //2
  val moviesProfitDF = moviesDF.select
  (
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr
  (
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  val moviesProfitDF3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  //3
  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comediesDF3.show
}
