package com.learn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("RandomData").master("local[1]").getOrCreate()

    val randomData = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("in/dataJan-10-2018.csv")

    randomData.printSchema()

    val onlyCyprus = randomData.select("email").filter(randomData.col("country") === "Cyprus")

    onlyCyprus.write.format("json").save("in/savedemail")

  }

}
