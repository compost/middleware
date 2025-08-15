package com.jada.models

import java.time.Instant

case class PlayerStore(
                        player_id: Option[String] = None,
                        //players
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
                        vip: Option[String] = None,
                        country_id: Option[String] = None,
                        currency_id: Option[String] = None,
                        currency_description: Option[String] = None,
                        //logins
                        last_login_datetime: Option[String] = None,
                        last_login_success: Option[String] = None,
                        //wallet
                        wallet_transaction_type_id: Option[String] = None,
                        wallet_transaction_status_id: Option[String] = None,
                        //wagering
                        wagering_transaction_type_id: Option[String] = None,
                        wagering_has_resulted: Option[String] = None,
                        wagering_ggr_amount: Option[String] = None,
                        //other
                        sent_customer_churn: Boolean = false,
                        last_activity_date: Option[Instant] = None,

                        //Should be removed
                        real_country_id: Option[String] = None,
                      )

object PlayerStore {
  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val playerStoreEncoder: Encoder[PlayerStore] = deriveEncoder
}
