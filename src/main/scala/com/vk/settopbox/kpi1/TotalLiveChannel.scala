package com.vk.settopbox.kpi1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.{Node, XML}

object TotalLiveChannel {

  def attributeValueEquals(value: String)(node: Node): Boolean = {
    node.attributes.exists(_.value.text == value)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId100 = inputData.filter(line => line.split(regexStr)(2).toInt == 100)

    val result = eId100.filter(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        val xmlData = XML.loadString(xmlStr)
        val nvs = xmlData \\ "nv"

        val liveChannelXml = nvs \\ "_" filter attributeValueEquals("ChannelType")
        val liveChannelRec = XML.loadString(liveChannelXml.toString())
        liveChannelRec.attribute("v").getOrElse(0).toString == "LiveTVMediaChannel"
      }
    ).collect().length

    println("Total LiveTVMediaChannel: - " + result)
    session.stop()
  }
}
