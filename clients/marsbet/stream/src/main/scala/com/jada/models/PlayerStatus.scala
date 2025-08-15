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

case class PlayerStatusBody(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    properties: PlayerStatusSQS,
    blockedUntil: Option[String],
    blocked: Option[Boolean]
)

object PlayerStatusBody {
  def apply(
      ps: PlayerStatus,
      mappingSelector: String,
      blocked: Option[Boolean]
  ): PlayerStatusBody = {
    new PlayerStatusBody(
      "USER_BLOCKED",
      ps.player_id.get,
      mappingSelector,
      PlayerStatusSQS(ps),
      ps.blocked_end_date
        .filter(!_.isBlank())
        .map(d => if (d.length() > 10) d.substring(0, 10) else d),
      blocked
    )
  }

  implicit val playerStatusSQSEncoder: Encoder[PlayerStatusSQS] =
    deriveEncoder

  implicit val playerStatusBodyEncoder: Encoder[PlayerStatusBody] =
    deriveEncoder
}
case class PlayerStatusSQS(
    blocked_reason: Option[String] = None
)

object PlayerStatusSQS {

  def apply(ps: PlayerStatus): PlayerStatusSQS = {
    new PlayerStatusSQS(
      blocked_reason = ps.player_blocked_reason
    )
  }
}
