package com.jada.models

import java.time.Instant

case class PlayerStore(
    player_id: Option[String] = None,
    // players_batch
    brand_id: Option[String] = None,
    brand_description: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    dob: Option[String] = None,
    country_description: Option[String] = None,
    currency_description: Option[String] = None,
    account_status: Option[String] = None,
    vip_status: Option[String] = None,
    login_name: Option[String] = None,
    domain_url: Option[String] = None,
    gender: Option[String] = None,
    mobile_status: Option[String] = None,
    email_verified: Option[String] = None,
    la_bcasinocom: Option[String] = None,
    la_boocasino: Option[String] = None,
    la_boocasinoca: Option[String] = None,
    la_galacticwins: Option[String] = None,
    la_mrfortune: Option[String] = None,
    marketing_seg: Option[String] = None,
    total_bonus_given: Option[String] = None,
    total_wd_amount: Option[String] = None,
    avg_deposit: Option[String] = None,
    avg_bet: Option[String] = None,
    avg_bet_eur: Option[String] = None,
    favorite_game: Option[String] = None,
    fav_game_cat: Option[String] = None,
    bonus_ratio: Option[String] = None,
    wd_ratio: Option[String] = None,
    turnover_ratio: Option[String] = None,
    ngr: Option[String] = None,
    chargeback: Option[String] = None,
    vip_seg: Option[String] = None,
    bonus_optout: Option[String] = None,
    total_number_of_deposits: Option[String] = None,
    total_deposit_amount: Option[String] = None,
    last_game_played: Option[String] = None,
    total_balance: Option[String] = None,
    bonus_balance: Option[String] = None,
    cp_balance: Option[String] = None,
    ftd_date: Option[String] = None,
    last_deposit_date: Option[String] = None,
    last_bonus_given: Option[String] = None,
    last_login_date: Option[String] = None,
    pending_withdrawal: Option[String] = None,
    pending_withdrawal_euro: Option[String] = None,
    total_deposit_amount_eur: Option[String] = None,
    total_bonus_given_eur: Option[String] = None,
    total_balance_eur: Option[String] = None,
    bonus_balance_eur: Option[String] = None,
    total_wd_amount_eur: Option[String] = None,
    avg_deposit_eur: Option[String] = None,
    cp_accumulated: Option[String] = None,
    id_status: Option[String] = None,
    poa_status: Option[String] = None,
    gcoins_balance: Option[String] = None,
    // technical
    player_info_from_batch: Boolean = false,
    player_batch_to_send: Boolean = false,
    // players only
    currency_id: Option[String] = None,
    verification_url: Option[String] = None,
    bonus_currency: Option[String] = None,
    reg_bonus_amount_cash: Option[String] = None,
    reg_bonus_amount_frb: Option[String] = None,
    ver_bonus_amount_cash: Option[String] = None,
    ver_bonus_amount_frb: Option[String] = None,
    reg_supported_games_frb: Option[String] = None,
    ver_supported_games_frb: Option[String] = None,
    // player_status
    player_send_self_exclusion: Boolean = false,
    nb_player_send_self_exclusion: Option[Int] = None,
    nb_player_send_self_exclusion_done: Option[Int] = None,
    player_send_timeout: Boolean = false,
    nb_player_send_timeout: Option[Int] = None,
    nb_player_send_timeout_done: Option[Int] = None,
    player_status_id: Option[String] = None,
    is_self_excluded_until: Option[String] = None,
    is_timeout_until: Option[String] = None,
    // consent
    email_consent: Option[String] = None,
    sms_consent: Option[String] = None
)
object PlayerStore {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder

}
