package com.vk.sum_of_numbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SumOfNumbers extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder().master("local[2]").getOrCreate()
  val data = session.sparkContext.textFile("in/prime_nums.text")
  val filterData = data.flatMap(line => line.split("\\s+"))
  val validNumbers = filterData.filter(number => !number.isEmpty)
  val intData = validNumbers.map(number => number.toInt)
  val result = intData.reduce((x, y) => x + y)
  println("result: " + result)
}
