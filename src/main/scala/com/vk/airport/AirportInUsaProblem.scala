package com.vk.airport

import com.vk.common.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AirportInUsaProblem {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val data = session.read.textFile("in/airports.text").rdd

    val filterdata = data.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")
    val result = filterdata.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })
    result.saveAsTextFile("out/airport_in_usa")
  }

}
