package com.codingchallenge.spark

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.StructType

object Task_3_2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("CodingChallenge")
      .master("local[*]")
      .getOrCreate()

    // Defining a schema for the Iris data
    val dataSchema = new StructType()
      .add("sepal_length", "Double")
      .add("sepal_width", "Double")
      .add("petal_length", "Double")
      .add("petal_width", "Double")
      .add("class", "String")

    // Loading the Iris.csv data into a DataFrame
    var data = spark.read
      .schema(dataSchema)
      .csv("data/iris.csv")


    // Combining all the different features columns into a single feature column
    val assembler = new VectorAssembler()
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
      .setOutputCol("features")

    data = assembler.transform(data)

    data = data.select("features", "class")

    // Converting the text classes into numerical indices
    val label_indexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
      .fit(data)

    data = label_indexer.transform(data)

    // Selecting only features and the label columns for input to the Model
    data = data.select("features", "label")

    // Set parameters to the Logistic Regression model
    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setTol(0.0001)
      .setRegParam(0.00001)
      .setFitIntercept(true)

    // Train the model with the input data
    val model = lr.fit(data)

    // Creating test data
    val test_data = Seq((5.1, 3.5, 1.4, 0.2),
                        (6.2, 3.4, 5.4, 2.3))

    // Creating a DataFrame and defining the schema for the test data
    val pred_data = spark.createDataFrame(test_data).toDF("sepal_length", "sepal_width", "petal_length", "petal_width")

    // Combining all the different features columns into a single feature column for the test data
    val pred_data_transformed = assembler.transform(pred_data)

    // Running the model on the test data for predictions
    val predictions = model.transform(pred_data_transformed.select("features"))

    // Writing the output predictions(Numerical values) to a file
    predictions.select("prediction").coalesce(1)
      .write
      .option("header", "true")
      .format("csv")
      .save("out/out_3_2")

    // Renaming the final output file to the expected naming
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path("out/out_3_2/part*"))(0).getPath().getName()
    fs.rename(new Path("out/out_3_2/" + file), new Path("out/out_3_2/out_3_2.txt"))

    spark.close()

  }

}
