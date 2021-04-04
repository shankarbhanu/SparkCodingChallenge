package com.codingchallenge.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object Task_2_2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("CodingChallenge")
      .master("local[*]")
      .getOrCreate()

    // Load the parquet file into a DataFrame
    val data = spark.read
      .parquet("data/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")

    // Creating a temporary table/view "airbnbtable" using the DataFrame
    data.createOrReplaceTempView("airbnbtable")

    // Selecting the minimum price, maximum price and the row count from the table
    val result = spark.sql("select min(price) as min_price" +
      ", max(price) as max_price" +
      ", count(1) as row_count" +
      " from airbnbtable")

    // Writing the results to an output file in the out directory
    result.write
      .option("header", "true")
      .format("csv")
      .save("out/out_2_2.txt")

    spark.close()

  }

}
