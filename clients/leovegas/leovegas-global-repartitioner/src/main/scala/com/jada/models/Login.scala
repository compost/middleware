package com.jada.models

case class Login(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    licenseuid: Option[String] = None,
    operator: Option[String] = None,
    login_datetime: Option[String] = None, 
    login_success: Option[String] = None, 
)
