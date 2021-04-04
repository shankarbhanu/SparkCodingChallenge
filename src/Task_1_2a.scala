package com.codingchallenge.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Task_1_2a {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("CodingChallenge")
      .master("local[*]")
      .getOrCreate()

    // Loading of the file into a rdd
    val lines = spark.sparkContext.textFile("data/groceries.csv")

    // Converting all lines to separate products and taking out the distinct products from the whole file
    val allProducts = lines.flatMap(x => x.split(",")).distinct()

    // Saving the distinct products to an output file in the out directory
    allProducts.coalesce(1).saveAsTextFile("out/out_1_2a.txt")

    spark.close()

  }

}
