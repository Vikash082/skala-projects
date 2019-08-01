package com.vk.airport_by_lat

import com.vk.common.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AirportByLatitude {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()

    val data = session.sparkContext.textFile("in/airports.text")
    val filterData = data.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val result = filterData.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    result.saveAsTextFile("out/airport_by_latitude")
  }

}
