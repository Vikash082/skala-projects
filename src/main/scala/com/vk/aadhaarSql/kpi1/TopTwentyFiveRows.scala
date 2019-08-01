package com.vk.aadhaarSql.kpi1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TopTwentyFiveRows {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("TopTwentyFiveRows").master("local[2]").getOrCreate()

    val data = session.read
      .option("header", "false")
      .option("inferSchema", value = true )
      .csv("in/aadhaar_data.csv")

    data.createOrReplaceTempView("aadhaar")

    val res = session.sql("select * from ( " +
      "select *, " +
      "row_number() over (partition by _c2 order by _c9 desc) as rn " +
      "from aadhaar) t where rn < 25")

    res.write.csv("out/kpi_adhaar")

  }

}
