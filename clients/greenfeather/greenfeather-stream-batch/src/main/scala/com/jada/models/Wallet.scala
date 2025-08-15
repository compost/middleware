package com.jada.models

//  {"transaction_id":"835083","brand_id":"73","transaction_type_id":"Deposit","transaction_status_id":"Accepted","amount":"30","transaction_datetime":"2023-06-03T05:54:06.0283537Z","player_id":"29763","currency_id":"1" }
case class Wallet(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    transaction_id: Option[String] = None,
    transaction_type_id: Option[String] = None,
    transaction_status_id: Option[String] = None,
    transaction_datetime: Option[String] = None,
    currency_description: Option[String] = None,
    amount_cur: Option[String] = None
)
