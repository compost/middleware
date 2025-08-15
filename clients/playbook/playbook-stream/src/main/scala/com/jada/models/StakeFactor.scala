package com.jada.models

import io.circe.Encoder
import io.circe.generic.semiauto._

case class StakeFactor(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    sf_inplay: Option[String] = None,
    sf_inplay_casino: Option[String] = None,
    sf_inplay_livecasino: Option[String] = None,
    sf_inplay_lottery: Option[String] = None,
    sf_inplay_virtualsports: Option[String] = None,
    sf_prematch: Option[String] = None,
    sf_prematch_casino: Option[String] = None,
    sf_prematch_livecasino: Option[String] = None,
    sf_prematch_lottery: Option[String] = None,
    sf_prematch_virtualsports: Option[String] = None
)

case class StakeFactorBody(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    properties: StakeFactorSQS
)

object StakeFactorBody {
  def apply(
      ps: StakeFactor,
      mappingSelector: String
  ): StakeFactorBody = {
    new StakeFactorBody(
      "GENERIC_USER",
      ps.player_id.get,
      mappingSelector,
      StakeFactorSQS(ps)
    )
  }

  implicit val stakeFactorSQSEncoder: Encoder[StakeFactorSQS] =
    deriveEncoder

  implicit val stakeFactorBodyEncoder: Encoder[StakeFactorBody] =
    deriveEncoder
}

case class StakeFactorSQS(
    brand_id: Option[String] = None,
    sf_inplay: Option[String] = None,
    sf_inplay_casino: Option[String] = None,
    sf_inplay_livecasino: Option[String] = None,
    sf_inplay_lottery: Option[String] = None,
    sf_inplay_virtualsports: Option[String] = None,
    sf_prematch: Option[String] = None,
    sf_prematch_casino: Option[String] = None,
    sf_prematch_livecasino: Option[String] = None,
    sf_prematch_lottery: Option[String] = None,
    sf_prematch_virtualsports: Option[String] = None
)

object StakeFactorSQS {

  def apply(ps: StakeFactor): StakeFactorSQS = {
    new StakeFactorSQS(
      brand_id = ps.brand_id,
      sf_inplay = ps.sf_inplay,
      sf_inplay_casino = ps.sf_inplay_casino,
      sf_inplay_livecasino = ps.sf_inplay_livecasino,
      sf_inplay_lottery = ps.sf_inplay_lottery,
      sf_inplay_virtualsports = ps.sf_inplay_virtualsports,
      sf_prematch = ps.sf_prematch,
      sf_prematch_casino = ps.sf_prematch_casino,
      sf_prematch_livecasino = ps.sf_prematch_livecasino,
      sf_prematch_lottery = ps.sf_prematch_lottery,
      sf_prematch_virtualsports = ps.sf_prematch_virtualsports
    )
  }
}
