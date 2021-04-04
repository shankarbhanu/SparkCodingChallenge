package com.codingchallenge.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object Task_1_2b {

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
    val products = lines.flatMap( x => x.split(",")).distinct().map(x => (x,1))

    // Creating a DataFrame containing the data of distinct products and the number 1 associated with each product
    val productsDF = spark.createDataFrame(products).toDF("Product", "number")

    // Creating a temporary table/view named "products" using the Products DataFrame
    productsDF.createOrReplaceTempView("products")

    // Calculating the total count of the products within the temporary table/view "products"
    val result = spark.sql("select count(1) as Count from products")

    // Saving the result to an output text file
    result.write
      .format("csv")
      .option("header", "true")
      .save("out/out_1_2b.txt")

    spark.close()

  }

}
