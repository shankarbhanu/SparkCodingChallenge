package com.codingchallenge.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._

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
    allProducts.coalesce(1).saveAsTextFile("out/out_1_2a")

    // Renaming the final output file to the expected naming
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path("out/out_1_2a/part*"))(0).getPath().getName()
    fs.rename(new Path("out/out_1_2a/" + file), new Path("out/out_1_2a/out_1_2a.txt"))

    spark.close()

  }

}
