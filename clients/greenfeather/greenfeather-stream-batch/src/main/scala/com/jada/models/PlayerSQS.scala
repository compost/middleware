package com.jada.models

import scala.util.Try

case class PlayerSQS(
                      originalId: Option[String] = None,
                      brand_id: Option[String] = None,
                      reg_date: Option[String] = None,
                      firstName: Option[String] = None,
                      lastName: Option[String] = None,
                      emailAddress: Option[String] = None,
                      mobileNumber: Option[String] = None,
                      language: Option[String] = None,
                      currency: Option[String] = None,
                      brand_description: Option[String] = None,
                      login_name: Option[String] = None,
                      domain_url: Option[String] = None,
                      verification_url: Option[String] = None,
                      bonus_currency: Option[String] = None,
                      reg_bonus_amount_cash: Option[String] = None,
                      reg_bonus_amount_frb: Option[String] = None,
                      ver_bonus_amount_cash: Option[String] = None,
                      ver_bonus_amount_frb: Option[String] = None,
                      reg_supported_games_frb: Option[String] = None,
                      ver_supported_games_frb: Option[String] = None
                    )

object PlayerSQS {

  def deserialize(data: Array[Byte]): PlayerSQS = {
    import io.circe._
    import io.circe.generic.auto._
    parser.decode[PlayerSQS](new String(data)).right.get
  }

  def apply(
             player: PlayerStore
           ): PlayerSQS = {
    new PlayerSQS(
      originalId = player.player_id,
      brand_id = player.brand_id,
      reg_date = player.reg_datetime.map(x => Try(x.substring(0,10)).getOrElse(x)),
      firstName = player.first_name,
      lastName = player.last_name,
      emailAddress = player.email,
      mobileNumber = player.phone_number,
      language = player.language,
      currency = player.currency_description,
      brand_description = player.brand_description,
      login_name = player.login_name,
      domain_url = player.domain_url,
      verification_url = player.verification_url,
      reg_bonus_amount_cash = player.reg_bonus_amount_cash,
      reg_bonus_amount_frb = player.reg_bonus_amount_frb,
      ver_bonus_amount_cash = player.ver_bonus_amount_cash,
      ver_bonus_amount_frb = player.ver_bonus_amount_frb,
      reg_supported_games_frb = player.reg_supported_games_frb,
      ver_supported_games_frb = player.ver_supported_games_frb
    )
  }
}
