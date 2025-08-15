package com.jada.models

// {"player_id":"15470","brand_id":"73","reg_datetime":"2023-04-03T23:13:01.5337680Z","first_name":"Russ","last_name":"Haxell","email":"russhaxell@gmail.com","phone_number":"+61482911714","language":"English","affiliate_id":"1","is_self_excluded":"false","first_dep_datetime":"2023-04-03T23:16:06Z","dob":"1995-09-14","country_id":"1","vip":"false","test_user":"false","currency_id":"1"}
case class Player(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
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
    country_id: Option[String] = None,
    country: Option[String] = None,
    vip: Option[String] = None,
    currency_id: Option[String] = None,
    test_user: Option[String] = None
)

case class PlayerExtended(
    originalId: Option[String] = None,
    brand_id: Option[String] = None,
    licenseuid: Option[String] = None,
    operator: Option[String] = None,
    country: Option[String] = None
)
