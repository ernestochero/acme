package com.utils

import scala.util.matching.Regex

object Pattern {
  val stagingRawDatePattern: Regex = "\\d{8}".r
  val masterDatePattern: Regex     = "(\\d{1,4}([.\\-])\\d{1,2}([.\\-])\\d{1,4})".r
}
