package com.jada.models

case class Wagering(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    transaction_type_id: Option[String] = None,
    has_resulted: Option[String] = None,
    ggr_amount: Option[String] = None,
    TOTAL_WIN_AMOUNT: Option[String] = None,
    bet_datetime: Option[String] = None
)
