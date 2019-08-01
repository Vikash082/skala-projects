package com.vk.aadhaarSql.kpi3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TopThreeResults {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("TopThreeStates").master("local[1]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    val topThreeStates = session.sql("select _c3, SUM(_c9) as TotalAdhaar from aadhaar " +
      "group by _c3 order by TotalAdhaar desc limit 3")
    topThreeStates.show()

    val topThreeAgency = session.sql("select _c2, SUM(_c9) as TotalAdhaar from aadhaar " +
      "group by _c2 order by TotalAdhaar desc limit 3")
    topThreeAgency.show()

    val totalResidents = session.sql("select COUNT(*) from aadhaar " +
      "where _c11 is not NULL AND _c12 is not NULL")
    totalResidents.show()

    val topThreeDistrictsEnrollment = session.sql("select _c4, SUM(_c9 + _c10) as TotalEnrollment " +
      "from aadhaar group by _c4 order by TotalEnrollment desc limit 3")
    topThreeDistrictsEnrollment.show()

    val totalAadhaarByState = session.sql("select _c3, SUM(_c9) as TotalAadhaar " +
      "from aadhaar group by _c3 order by TotalAadhaar desc")
    totalAadhaarByState.show()

  }

}
