package com.jada.betway.model

import java.time.Instant

case class StoreData(
                      player_id: Option[String],
                      playerId: Option[String],
                      originalId: Option[String],
                      brand_id: Option[String],
                      reg_datetime: Option[String],
                      first_name: Option[String],
                      last_name: Option[String],
                      email: Option[String],
                      phone_number: Option[String],
                      language: Option[String],
                      affiliate_id: Option[String],
                      is_self_excluded: Option[String],
                      first_dep_datetime: Option[String],
                      dob: Option[String],
                      country_id: Option[String],
                      vip: Option[String],
                      test_user: Option[String],
                      currency_id: Option[String],
                      last_login_datetime: Option[String],
                      nick_name: Option[String],
                      gender_id: Option[String],
                      gender_description: Option[String],
                      address: Option[String],
                      zip_code: Option[String],
                      city: Option[String],
                      receive_email: Option[String],
                      device_type_id: Option[String],
                      device_type_description: Option[String],
                      last_deposit_datetime: Option[String],
                      player_status_id: Option[String],
                      player_status_description: Option[String],
                      user_validated: Option[String],
                      user_validated_time: Option[String]
)
