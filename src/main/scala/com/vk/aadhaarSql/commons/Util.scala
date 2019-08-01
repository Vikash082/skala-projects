package com.vk.aadhaarSql.commons

case class Data(Date: String, Registrar: String, PrivateAgency: String, State: String, District: String,
                SubDistrict: String, PinCode: String, Gender: String, Age: Int, AadharGenerated: Int,
                Rejected: Int, MobileNo: Int, EmailId: String)
