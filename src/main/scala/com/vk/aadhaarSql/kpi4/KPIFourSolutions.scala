package com.vk.aadhaarSql.kpi4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object KPIFourSolutions {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("KPIFourSolutions").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    val uniquePincode = session.sql("select COUNT(DISTINCT _c6) as TotalPincode from aadhaar")
    uniquePincode.show()

    val aadhaarRejected = session.sql("select SUM(_c10) from aadhaar where _c3 " +
      "in ('Uttar Pradesh', 'Maharashtra') ")
    aadhaarRejected.show()
  }

}
