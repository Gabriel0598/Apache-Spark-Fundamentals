package Streaming

import New_Ex.part2dataframes.DataFrameBasics.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import delta.tables.*
object SparkStreaming extends App {
  //creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate() //Constructor of spark session;

  spark.sql("CREATE TABLE MyExternalTable USING DELTA LOCATION '/mydata'")

  DeltaTable.create(spark)
  .tableName("default.ManagedProducts")
  .addColumn("Productid", "INT")
  .addColumn("ProductName", "STRING")
  .addColumn("Category", "STRING")
  .addColumn("Price", "FLOAT")
  .execute()

  //Load a streaming dataframe from the Delta Table
  val stream_df = spark.readStream.format("delta")
    .option("ignoreChanges", "true")
    .load("/delta/internetorders")

  //Now you can process the streaming data in the dataframe
  // for example, show it:
    stream_df.writeStream
  .outputMode("append")
  .format("console")
  .start()

  //Create a stream that reads JSON data from a folder
   val inputPath = "/streamingdata/";

  val jsonSchema = StructType([
    StructField("device", StringType(), False),
    StructField("status", StringType(), False)
  ])
  val stream_df = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

  //Write the stream to a delta table
  val table_path = "/delta/devicetable"
  val checkpoint_path = "/delta/checkpoint"

  val delta_stream = stream_df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(table_path)
}
