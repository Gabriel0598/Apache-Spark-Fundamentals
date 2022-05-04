package New_Ex.part3typedatabase

import com.sun.jdi.FloatType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

object CommonTypes extends App
{
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  //booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama" //=== is the same that equalTo
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)
  // * multiple ways of filtering

  val moviesWithGodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  //filter on a boolean column
  moviesWithGodnessFlagsDF.where("good_movie") //where(col("good_movie") === "true")

  //negations
  moviesWithGodnessFlagsDF.where(not(col("good_movie")))

  //Numbers
  val floatType = FloatType
  //math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  //correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /*corr is and ACTION*/)

  //Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Capitalization: initcap, lower, upper
  carsDF.select(initCap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  //regex
  val regexString = "volkswagen|vw"
  carsDF.select
  (
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.select
  (
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's car").as("regex_replace")
  ).show()

  /*
  * Exercise
  *
  * Filter the cars DF by a list of car names obtained by an API call
  *
  * */
  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  //version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") //Volkswagen | Mercedes-Benz | Ford
  carsDF.select
  (
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
    ).where(col("regex_extract") =!= "").drop("regex_extract").show()

  //version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show
}
