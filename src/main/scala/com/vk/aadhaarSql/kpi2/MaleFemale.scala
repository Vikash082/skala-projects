package com.vk.aadhaarSql.kpi2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MaleFemale {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("MaleFemale").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    val male = session.sql("select COUNT(*) as TotalMale from (select _c7 from aadhaar where _c7 == 'M')")
    male.show()
    val female = session.sql("select COUNT(*) as TotalFemale from (select _c7 from aadhaar where _c7 == 'F')")
    female.show()
  }

}
