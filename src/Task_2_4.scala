package com.codingchallenge.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.hadoop.fs._

object Task_2_4 {

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

    // Selecting the number of people that can be accommodated where price is the minimum price and rating is the maximum rating
    // Note: The same assumption as in Task_2_3 where review_scores_accuracy column is considered for the rating
    val result = spark.sql("select accommodates " +
      "from airbnbtable " +
      "where price = (select min(price) from airbnbtable) " +
      "and review_scores_accuracy = (select max(review_scores_accuracy) from airbnbtable)")

    // Writing the results into an output file
    // Note: There were two records with the same price and rating hence the output file consists of two rows
    result.write
      .option("header", "true")
      .format("csv")
      .save("out/out_2_4")

    // Renaming the final output file to the expected naming
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path("out/out_2_4/part*"))(0).getPath().getName()
    fs.rename(new Path("out/out_2_4/" + file), new Path("out/out_2_4/out_2_4.txt"))

    spark.close()

  }

}
