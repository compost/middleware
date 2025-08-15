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
    blockedUntil: Option[String]
)

object PlayerStatusBody {
  def apply(
      ps: PlayerStatus,
      mappingSelector: String
  ): PlayerStatusBody = {
    new PlayerStatusBody(
      "USER_BLOCKED",
      ps.player_id.get,
      mappingSelector,
      PlayerStatusSQS(ps),
      ps.blocked_end_date
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
    player_blocked_reason: Option[String] = None,
    blocked_start_date: Option[String] = None
)

object PlayerStatusSQS {

  def apply(ps: PlayerStatus): PlayerStatusSQS = {
    new PlayerStatusSQS(
      brand_id = ps.brand_id,
      player_status_id = ps.player_status_id,
      player_blocked_reason = ps.player_blocked_reason,
      blocked_start_date = ps.blocked_start_date
    )
  }
}

case class BlockUntilBody(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    blockedUntil: Option[String]
)

object BlockUntilBody {
  def apply(
      ps: PlayerStatus,
      mappingSelector: String
  ): BlockUntilBody = {
    new BlockUntilBody(
      "USER_BLOCKED",
      ps.player_id.get,
      mappingSelector,
      ps.blocked_end_date
    )
  }

  implicit val blockUntilBodyEncoder: Encoder[BlockUntilBody] =
    deriveEncoder
}

case class BlockedBody(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    blocked: Boolean,
    player_blocked_reason: Option[String]
)

object BlockedBody {
  def apply(
      ps: PlayerStatus,
      blocked: Boolean,
      mappingSelector: String
  ): BlockedBody = {
    new BlockedBody(
      "USER_BLOCKED_TOGGLE",
      ps.player_id.get,
      mappingSelector,
      blocked,
      ps.player_blocked_reason
    )
  }

  implicit val blockedBodyEncoder: Encoder[BlockedBody] =
    deriveEncoder
}
