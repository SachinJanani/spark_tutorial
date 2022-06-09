package com.unravel.sparktutorial

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object wordcount_ds {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val text_file = spark.read.text("src/main/resources/com/unravel/sparktutorial/data.txt")
    text_file.show()

    val flatMapDF = text_file.flatMap(r => r.getString(0).split(" "))(Encoders.STRING)
    flatMapDF.groupBy("value").count().show()
  }

}
