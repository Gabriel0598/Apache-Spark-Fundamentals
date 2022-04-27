package New_Ex.part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataFrameBasics extends App
{
  //creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate() //Constructor of spark session;

  //reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  //showing a DF
  firstDF.show()
  firstDF.printSchema()

  //get rows
  firstDF.take(10).foreach(println)

  //spark types
  val longType = LongType //describe the data

  //schema
  val carsSchema = StructType
  (
    Array
    (
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", IntegerType),
      StructField("Cylinders", IntegerType),
      StructField("Displacement", IntegerType),
      StructField("Horsepower", IntegerType),
      StructField("Weight_in_lbs", IntegerType),
      StructField("Accelaration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
    )

  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  //read a DF with your schema
  val carsDFwithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  //create rows by hand
  //It's possible create a dataframe manually with a sequence of row, sequence of tuples;
  //val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  //create DF from tuples
  var cars = Seq
  (
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) //schame auto-inferred

  //note: DFs have schemas, rows do not

  //create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /*
  * Exercise:
  * 1) Create a manual DF describing smartphones
  *   -make
  *   -model
  *   -screen dimension
  *   -camera megapixels
  *
  * 2) Read another file from the data/ folder, e.g. movies.json
  *   -print its schema
  *   -count the number of rows, call count()
  * */

  //Create manual DF
  //1
  val smarphones = Seq
  (
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "Iphone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0)
  )

  val smartphonesDF = smarphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
  smartphonesDF.show()

  //2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"The movies DF has ${moviesDF.count()} rows")
}