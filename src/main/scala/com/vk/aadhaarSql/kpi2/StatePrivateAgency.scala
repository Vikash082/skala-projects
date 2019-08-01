package com.vk.aadhaarSql.kpi2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StatePrivateAgency {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StatePrivateAgency").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    val res = session.sql("select _c3, _c2 from aadhaar group by _c3, _c2 order by _c3")
    res.collect().foreach(println)
  }
}
