package com.jada.models

case class Wagering(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    licenseuid: Option[String] = None,
    operator: Option[String] = None,
    bet_id: Option[String] = None,
    wager_amount_cash: Option[String] = None,
    wager_amount_bonus: Option[String] = None,
    wager_amount_freebet: Option[String] = None,
    transaction_type_id: Option[String] = None,
    ggr_amount: Option[String] = None,
    game_id: Option[String] = None,
    vertical_id: Option[String] = None,
    price: Option[String] = None,
    bet_datetime: Option[String] = None,
    currency_id: Option[String] = None,
    has_resulted: Option[String] = None,
    result_datetime: Option[String] = None,
    result_id: Option[String] = None
)

