package com.jada.models

case class Wagering(
    bet_id: Option[String] = None,
    bet_datetime: Option[String] = None,
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    licenseuid: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    operator: Option[String] = None,
    result_id: Option[String],
    result_datetime: Option[String],
    has_resulted: Option[String],
    vertical_id: Option[String],
    ggr_amount: Option[String]
)
