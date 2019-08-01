package com.vk.aadhaarDf.common


object utils {

  case class Adhaar(
                     date: Option[Int],
                     registrar: String,
                     private_agency: String,
                     state: String,
                     district: String,
                     sub_district: String,
                     pin_code: String,
                     gender: String,
                     age: Option[Int],
                     aadhaar_generated: Option[Int],
                     aadhaar_rejected: Option[Int],
                     mobile_number: Option[Int],
                     email_id: Option[Int]
                   )

  object StringImplicts {
    implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
    }
  }



}
