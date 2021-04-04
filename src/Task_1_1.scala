package com.codingchallenge.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Task_1_1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("CodingChallenge")
      .master("local[*]")
      .getOrCreate()

    //The file has been downloaded to the data folder within the project structure
    // Loading of the file into a rdd
    val lines = spark.sparkContext.textFile("data/groceries.csv")

    //Prints all the lines within the csv file
    lines.foreach(println)

    spark.close()

  }

}
