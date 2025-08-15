package com.jada.models

//  {"transaction_id":"835083","brand_id":"73","transaction_type_id":"Deposit","transaction_status_id":"Accepted","amount":"30","transaction_datetime":"2023-06-03T05:54:06.0283537Z","player_id":"29763","currency_id":"1" }
case class Wagering(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    bet_datetime: Option[String] = None,
    result_datetime: Option[String] = None,
    total_balance: Option[String] = None,
    kafka_timestamp: Option[String] = None
)
