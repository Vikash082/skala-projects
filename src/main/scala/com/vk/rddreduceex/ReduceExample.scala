package com.vk.rddreduceex

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReduceExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[2]").getOrCreate()

    val input = List(1, 2, 3, 4, 5)

    val intRdd = spark.sparkContext.parallelize(input)

    val product = intRdd.reduce((x, y) => x * y)

    println("product is :  " + product)

  }

}
