package com.vk.settopbox.kpi3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.{Node, XML}

object MaxPriceGroupByOffer {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId102or113 = inputData.filter(line => {
      val id = line.split(regexStr)(2).toInt
      id == 102 || id == 113
    })

    val result = eId102or113.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        var price = 0F
        var offerId = ""
        val xmlBody = XML.loadString(xmlStr)
        for (nv <- xmlBody.child) {
          val nvrec = XML.loadString(nv.toString())
          val nvKey = nvrec.attribute("n").getOrElse("").toString
          val nvVal = nvrec.attribute("v").getOrElse("").toString

          if (nvKey == "Price" && nvVal != "") {
            price = nvVal.toFloat
          }

          if (nvKey == "OfferId") {
            offerId = nvVal
          }
        }
        (offerId, price)
      })
      .reduceByKey(math.max(_, _))
      .sortBy(rec => rec._2, false)

//    for ((k, v) <- result) println(k, v)
    result.saveAsTextFile("out/kpi3")

  }

}
