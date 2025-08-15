package com.jada.models

import java.time.Instant

case class PlayerStore(
    player_id: Option[String] = None,
    // players
    brand_id: Option[String] = None,
    reg_datetime: Option[String] = None,
    first_name: Option[String] = None,
    last_name: Option[String] = None,
    email: Option[String] = None,
    phone_number: Option[String] = None,
    language: Option[String] = None,
    affiliate_id: Option[String] = None,
    is_self_excluded: Option[String] = None,
    first_dep_datetime: Option[String] = None,
    dob: Option[String] = None,
    country_id: Option[String] = None,
    vip: Option[String] = None,
    currency_id: Option[String] = None,
    receive_email: Option[String] = None,
    receive_sms: Option[String] = None,
    test_user: Option[String] = None,
    nick_name: Option[String] = None,
    gender_id: Option[String] = None,
    gender_description: Option[String] = None,
    address: Option[String] = None,
    zip_code: Option[String] = None,
    city: Option[String] = None,
    device_type_id: Option[String] = None,
    device_type_description: Option[String] = None,
    player_status_id: Option[String] = None,
    player_status_description: Option[String] = None,
    user_validated: Option[String] = None,
    user_validated_time: Option[String] = None,
    Benefit: Option[String] = None,
    // logins
    last_login_datetime: Option[String] = None,
    last_login_success: Option[String] = None,
    // wallet
    wallet_transaction_type_id: Option[String] = None,
    wallet_transaction_status_id: Option[String] = None,
    // wagering
    wagering_transaction_type_id: Option[String] = None,
    wagering_has_resulted: Option[String] = None,
    wagering_ggr_amount: Option[String] = None,
    wagering_TOTAL_WIN_AMOUNT: Option[String] = None,
    // other
    sent_customer_churn: Option[Boolean] = Option(false),
    last_activity_date: Option[Instant] = None,
    // User_consent_update
    email_consented: Option[String] = None,
    sms_consented: Option[String] = None,
    // User_consent_update
    history_email_consented: Option[String] = None,
    history_sms_consented: Option[String] = None,
    SITE_ID: Option[String] = None,
    batchName: Option[String] = None
)
object PlayerStore {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder

}
