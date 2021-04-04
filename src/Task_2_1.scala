package com.codingchallenge.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object Task_2_1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("CodingChallenge")
      .master("local[*]")
      .getOrCreate()

    // Downloaded the file to the data folder withing the project structure
    // Load the parquet file into a DataFrame
    val lines = spark.read.parquet("data/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")

    // Printing the schema of the DataFrame
    lines.printSchema()

    // Printing the top 5 records withing the DataFrame
    lines.show(5)

    spark.close()

  }

}
