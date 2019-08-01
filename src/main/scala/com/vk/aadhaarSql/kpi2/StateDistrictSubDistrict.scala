package com.vk.aadhaarSql.kpi2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StateDistrictSubDistrict {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("StateDistrictSubDistrict").master("local[2]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

//    val res = session.sql("select SUM(count) from (select COUNT(DISTINCT _c3) as count from" +
//      " aadhaar group by _c3)")
    val states = session.sql("select COUNT(*) from (select DISTINCT _c3 from aadhaar)")
    states.show()

    val districts = session.sql("select _c3, count(DISTINCT _c4) as Count from aadhaar group by _c3 ")
    districts.show()

    val subDistricts = session.sql("select  _c4, count(DISTINCT _c5) as SubDistrictsCount from " +
      "aadhaar group by _c4")
    subDistricts.show()
  }

}
