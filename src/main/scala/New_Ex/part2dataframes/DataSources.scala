package New_Ex.part2dataframes

import New_Ex.part2dataframes.DataFrameBasics.spark
import com.sun.jdi.DoubleType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*

import java.sql.Struct

object DataSources extends App
{
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master","local")
    .getOrCreate()

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
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
    )

  /*
  * Reading a DF:
  - format
  - schema or inferSchema = true
  - zero or more options
  * */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) //enforce a schema
    .option("mode","failFast") //dropMalformed, permissive (default)
    .option("path","src/main/resources/data/cars.json")
    .load()

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map
    (
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
  Writing DFs
  -format
  -save mode = overwrite, append, ignore, errorIfExists
  -path
  -zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  //JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") //couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  //CSV flags
  val stockSchema = StructType (Array
  (
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructFIeld("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(StockSchema)
    .option("dateFormat", "MM-dd-yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  //Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  //Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //Reading from a remote DB

}
