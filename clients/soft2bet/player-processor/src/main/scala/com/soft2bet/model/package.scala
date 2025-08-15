package com.soft2bet

import java.text.SimpleDateFormat

package object model {
  val yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd")
  def formatYYYYMMDD(t: Long): String = {
    yyyyMMdd.format(t)
  }
  def keepYYYYMMDD(s: String): String = {
    if (s.length() > 10) {
      s.substring(0, 10)
    } else {
      s
    }
  }

}
