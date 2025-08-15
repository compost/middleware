package com.jada.models

import io.circe.Encoder
import io.circe.generic.semiauto._

case class PlayerStatus(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    player_status_id: Option[String] = None,
    player_blocked_reason: Option[String] = None,
    blocked_start_date: Option[String] = None,
    blocked_end_date: Option[String] = None
)
