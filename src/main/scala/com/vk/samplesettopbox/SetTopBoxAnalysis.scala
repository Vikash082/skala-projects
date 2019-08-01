package com.vk.samplesettopbox

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.xml.{Node, XML}

object SetTopBoxAnalysis {

  def attributeValueEquals(value: String)(node: Node): Boolean = {
    node.attributes.exists(_.value.text == value)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().master("local[2]").getOrCreate()

//    Input data format:
//
//    Server-Unique-Id - 11001
//    Request-Type - 1
//    Event-Id - 100
//    Timestamp - 2015-06-05 22:35:21.543
//    XML with tags of name and value - <d><nv n="ExtStationID" v="Station/FYI Television, Inc./25102" /><nv n="MediaDesc" v="19b8f4c0-92ce-44a7-a403-df4ee413aca9" /><nv n="ChannelNumber" v="1366" /><nv n="Duration" v="24375" /><nv n="IsTunedToService" v="True" /><nv n="StreamSelection" v="FULLSCREEN_PRIMARY" /><nv n="ChannelType" v="LiveTVMediaChannel" /><nv n="TuneID" v="636007629215440000" /></d>
//    Device Id - 0122648d-4352-4eec-9327-effae0c34ef2
//    Secondary Timestamp - 2016060601

//    val data = "11001^1^100^2015-06-05 22:35:21.543^<d><nv n=\"ExtStationID\" v=\"Station/FYI Television, Inc./25102\" /><nv n=\"MediaDesc\" v=\"19b8f4c0-92ce-44a7-a403-df4ee413aca9\" /><nv n=\"ChannelNumber\" v=\"1366\" /><nv n=\"Duration\" v=\"24375\" /><nv n=\"IsTunedToService\" v=\"True\" /><nv n=\"StreamSelection\" v=\"FULLSCREEN_PRIMARY\" /><nv n=\"ChannelType\" v=\"LiveTVMediaChannel\" /><nv n=\"TuneID\" v=\"636007629215440000\" /></d>^0122648d-4352-4eec-9327-effae0c34ef2^2016060601"
//    val tokens = data.split("\\^")
//    for (token <- tokens) println(token)

    val inputData = session.sparkContext.textFile("in/Set_Top_Box_Data.txt")
    val regexStr = "\\^"
    val eId100 = inputData.filter(line => line.split(regexStr)(2).toInt == 100)

    val result = eId100.map(
      line => {
        val splits = line.split(regexStr)
        val xmlStr = splits(4)
        val deviceId = splits(5)

        val xmlData = XML.loadString(xmlStr)
        val nvs = xmlData \\ "nv"
        val durationXml = nvs \\ "_" filter attributeValueEquals("Duration")
        val durationRec = XML.loadString(durationXml.toString())

        (deviceId, durationRec.attribute("v").getOrElse(0).toString.toInt)
      }
    )
      .reduceByKey(math.max(_, _))
      .sortBy(rec => rec._2, ascending = false)
      .take(5)

    for (res <- result) println(res)
    session.stop()

  }

}
