package com.codingchallenge.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._

object Task_1_3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("codingChallenge")
      .master("local[1]")
      .getOrCreate()

    // Loading of the file into a rdd
    val lines = spark.sparkContext.textFile("data/groceries.csv")

    // Splitting the lines withing the rdd into individual products
    val products = lines.flatMap(x => x.split(","))

    // Getting the frequency count of each product within the rdd
    val productsCount = products.map( x => (x,1)).reduceByKey( (x,y) => x + y)

    // Sorting the products based on the frequency count in a descending order
    val productsSorted = productsCount.sortBy(_._2, false)

    // Saving the top 5 products with high frequency count into a text file
    spark.createDataFrame(productsSorted.take(5)).rdd.saveAsTextFile("out/out_1_3")

    // Renaming the outfile file to the expected naming
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path("out/out_1_3/part*"))(0).getPath().getName()
    fs.rename(new Path("out/out_1_3/" + file), new Path("out/out_1_3/out_1_3.txt"))

    spark.close()

  }

}
