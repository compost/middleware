package com.jada.models

case class Player(
                   player_id: Option[String] = None,
                   brand_id: Option[String] = None,
                   reg_datetime: Option[String] = None,
                   first_name: Option[String] = None,
                   last_name: Option[String] = None,
                   email: Option[String] = None,
                   phone_number: Option[String] = None,
                   language: Option[String] = None,
                   currency_id: Option[String] = None,
                   currency_description: Option[String] = None,
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
