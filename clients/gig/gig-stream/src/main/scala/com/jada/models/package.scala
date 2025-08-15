package com.jada


package object models {
def keepYYYYMMDD(s:String) : String = {
  if(s.length() > 10) {
    s.substring(0, 10)
  } else {
    s
  }
}

}
