package com.jada.models

//{"bet_id":"1219151373","brand_id":"73","player_id":"64602","wager_amount_cash":"0.6","wager_amount_bonus":"0","wager_amount_freebet":"0","transaction_type_id":"Bet","ggr_amount":"-0.6","game_id":"19609","vertical_id":"1","price":"1","bet_datetime":"2023-10-03T14:40:55.9937399Z","currency_id":"1","has_resulted":"true","result_datetime":"2023-10-03T14:40:55.993739900Z","result_id":"2"} 
case class Wagering(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    transaction_type_id: Option[String] = None,
    has_resulted: Option[String] = None,
    ggr_amount: Option[String] = None,
    bet_datetime: Option[String] = None,

)

