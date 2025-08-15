package com.jada.models 

case class Currency(val currency_description: Option[String], 
  val currency_id: Option[String], val brand_id: Option[String], val brand_description: Option[String])
