package com.soft2bet.model

case class AccountFrozen(
    player_id: Option[String],
    brand_id: Option[String],
    BlockReason: Option[String],
    IsBlocked: Option[String],
    BlockedCasino: Option[String],
    BlockedSport: Option[String]
)
