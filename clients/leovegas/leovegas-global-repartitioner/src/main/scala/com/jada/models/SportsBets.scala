package com.jada.models

case class SportsBets(
    bet_id: Option[String],
    championship_id: Option[String],
    market_id: Option[String],
    player_id: Option[String],
    event_id: Option[String],
    is_live: Option[String],
    brand_id: Option[String],
    country_id: Option[String],
    country: Option[String],
    licenseuid: Option[String],
    operator: Option[String]
)
