package com.jada.models

case class PlayerConsent(
    `type`: Option[String],
    player_id: Option[String],
    consented: Option[String],
    brand_id: Option[String],
    channel: Option[String]
)
