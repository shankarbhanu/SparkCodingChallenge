package com.codingchallenge.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import  org.apache.hadoop.fs._

object Task_2_3 {

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

    // Selecting the average bathrooms and average bedrooms where price > 5000 and review score is exactly 10
    /* Note: There are many columns named as review_scores*** and in particular there were two columns
             with names review_scores_rating and review_scores_accuracy. In this scenario I have considered
             the review_scores_accuracy as my filtering column as review_scores_rating column did not have any
             records with a rating of 10 */
    val result = spark.sql("select avg(bathrooms) as avg_bathrooms" +
      ", avg(bedrooms) as avg_bedrooms " +
      "from airbnbtable " +
      "where price > 5000 and review_scores_accuracy = 10")

    // Writing the results into an output file
    result.write
      .option("header", "true")
      .format("csv")
      .save("out/out_2_3")

    // Renaming the final output file to the expected naming
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path("out/out_2_3/part*"))(0).getPath().getName()
    fs.rename(new Path("out/out_2_3/" + file), new Path("out/out_2_3/out_2_3.txt"))

    spark.close()

  }

}
