package com.jada.models

case class Player(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    licenseuid: Option[String] = None,
    operator: Option[String] = None,
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
    test_user: Option[String] = None,
    currency_id: Option[String] = None
)

