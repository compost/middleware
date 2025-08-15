package com.jada.models

import io.circe.Encoder
import io.circe.generic.semiauto._

case class PlayerStatus(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    player_status_id: Option[String] = None,
    player_blocked_reason_id: Option[String] = None,
    blocked_start_date: Option[String] = None,
    blocked_end_date: Option[String] = None
)

case class PlayerStatusBody(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    properties: PlayerStatusSQS
)

object PlayerStatusBody {
  def apply(
      ps: PlayerStatus,
      mappingSelector: String
  ): PlayerStatusBody = {
    new PlayerStatusBody(
      "GENERIC_USER",
      ps.player_id.get,
      mappingSelector,
      PlayerStatusSQS(ps)
    )
  }

  implicit val playerStatusSQSEncoder: Encoder[PlayerStatusSQS] =
    deriveEncoder

  implicit val playerStatusBodyEncoder: Encoder[PlayerStatusBody] =
    deriveEncoder
}
case class PlayerStatusSQS(
    brand_id: Option[String] = None,
    player_status_id: Option[String] = None,
    player_blocked_reason_id: Option[String] = None,
    blocked_start_date: Option[String] = None,
    blocked_end_date: Option[String] = None
)

object PlayerStatusSQS {

  def apply(ps: PlayerStatus): PlayerStatusSQS = {
    new PlayerStatusSQS(
      brand_id = ps.brand_id,
      player_status_id = ps.player_status_id,
      player_blocked_reason_id = ps.player_blocked_reason_id,
      blocked_start_date = ps.blocked_start_date,
      blocked_end_date = ps.blocked_end_date,
    )
  }
}
