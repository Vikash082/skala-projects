package com.vk.common

object Utils {
  // a regular expression which matches commas but not commas within double quotations
  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
}
