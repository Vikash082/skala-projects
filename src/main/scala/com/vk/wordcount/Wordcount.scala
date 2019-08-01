package com.vk.wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Wordcount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("Wordcount")
      .master("local[2]")
      .getOrCreate()

    val lines = spark.read.textFile("in/about-dataflair.txt").rdd
    val result = {
      lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y)
    }
    result.saveAsTextFile("out/wordcount_out")
  }

}
