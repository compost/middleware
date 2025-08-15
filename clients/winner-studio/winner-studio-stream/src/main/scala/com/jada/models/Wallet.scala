package com.jada.models

//  {"transaction_id":"835083","brand_id":"73","transaction_type_id":"Deposit","transaction_status_id":"Accepted","amount":"30","transaction_datetime":"2023-06-03T05:54:06.0283537Z","player_id":"29763","currency_id":"1" }
case class Wallet(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    transaction_type_id: Option[String] = None,
    transaction_status_id: Option[String] = None,
    transaction_datetime: Option[String] = None,
    amount: Option[String] = None
)

object Wallet {

  val Successful = "successful"
  val TriggerOnTransactionTypeIds = Set(
    "shop_charge",
    "promotion_deposit",
    "three_deposit_1",
    "three_deposit_2",
    "three_deposit_3",
    "unlock_deposit_2",
    "starter_deposit",
    "miss_deposit",
    "day7pack_deposit_1",
    "day7pack_deposit_2",
    "day7pack_deposit_3",
    "day7pack_deposit_4",
    "day7pack_deposit_5",
    "day7pack_deposit_6",
    "balance_not_enough_1",
    "balance_not_enough_2",
    "vote_charge_1",
    "vote_charge_2",
    "repeat_charge",
    "double_reward",
    "double_room",
    "lucky_bonus",
    "safe_box_deposit",
    "lucky_egg_2",
    "activity_charge",
    "progression_recharge_1",
    "progression_recharge_2",
    "progression_recharge_3",
    "progression_recharge_4",
    "progression_recharge_5",
    "progression_recharge_6",
    "progression_recharge_7",
    "new_unlock"
  )
}
