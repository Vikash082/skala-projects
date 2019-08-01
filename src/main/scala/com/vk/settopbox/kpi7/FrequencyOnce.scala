package com.vk.settopbox.kpi7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.XML

object FrequencyOnce {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId115or118 = inputData.filter(line => {
      val eventId = line.split(regexStr)(2).toInt
      eventId == 115 || eventId == 118
    })
      .filter { line => {
        val tokens = line.split(regexStr)
        val body = tokens(4)
        body.contains("<d>")
      }
      }

    val records = eId115or118.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        var frequency = ""
        val xmlBody = XML.loadString(xmlStr.toString)

        for (nv <- xmlBody.child) {
          val nvRec = XML.loadString(nv.toString())
          val nvKey = nvRec.attribute("n").getOrElse("").toString
          val nvVal = nvRec.attribute("v").getOrElse("").toString

          if (nvKey == "Frequency" && nvVal == "Once") frequency = "Once"
        }
        (frequency, 1)
      })
      .reduceByKey((x, y) => x + y)
      .filter(rec => rec._1 == "Once")

    records.saveAsTextFile("out/kpi7Once")
  }
}
