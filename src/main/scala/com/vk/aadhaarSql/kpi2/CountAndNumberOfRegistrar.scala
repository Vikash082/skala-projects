package com.vk.aadhaarSql.kpi2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CountAndNumberOfRegistrar {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("CountNumberOfRegistrar").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    val res = session.sql("select _c1, COUNT(_c1) as count from aadhaar group by _c1")
    res.show()
  }

}
