package com.leovegas.model

case class Wagering(
    bet_id: Option[String],
    player_id: Option[String],
    has_resulted: Option[String],
    result_id: Option[String],
    result_datetime: Option[String],
    vertical_id: Option[String],
    brand_id: Option[String],
    country_id: Option[String],
    country: Option[String],
    licenseuid: Option[String],
    operator: Option[String]
)
