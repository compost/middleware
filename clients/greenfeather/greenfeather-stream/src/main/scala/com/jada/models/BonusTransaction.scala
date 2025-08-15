package com.jada.models

case class BonusTransaction(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    bonus_transaction_id: Option[String] = None,
    transaction_datetime: Option[String] = None,
    bonus_type: Option[String] = None,
    bonus_amount_cur: Option[String] = None,
    bonus_currency: Option[String] = None,
    frb_rounds: Option[String] = None,
    supported_games: Option[String] = None,
    kafka_timestamp: Option[String] = None
)
