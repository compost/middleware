package com.jada.models

// {"player_id":"15470","brand_id":"73","reg_datetime":"2023-04-03T23:13:01.5337680Z","first_name":"Russ","last_name":"Haxell","email":"russhaxell@gmail.com","phone_number":"+61482911714","language":"English","affiliate_id":"1","is_self_excluded":"false","first_dep_datetime":"2023-04-03T23:16:06Z","dob":"1995-09-14","country_id":"1","vip":"false","test_user":"false","currency_id":"1"}
case class Player(
    player_id: Option[String],
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
    VIP: Option[String],
    test_user: Option[String],
    currency_description: Option[String],
    country_description: Option[String],
    is_playbreak: Option[String],
    playbreak_expiry: Option[String],
    postcode: Option[String],
    promo_id: Option[String],
    promo_banned: Option[String],
    city: Option[String],
    is_deposited_restricted: Option[String]
)

//{"BRAND_ID":170,"PLAYER_BLOCKED_REASON_DESCRIPTION":"Unknown","PLAYER_ID":"91432"}
case class CI1359BlockedReason(
    BRAND_ID: Option[Int] = None,
    PLAYER_ID: Option[String] = None,
    PLAYER_BLOCKED_REASON_DESCRIPTION: Option[String] = None
)
case class PlayerDB(
    AFFILIATEID: Option[String] = None, // "146"
    BRAND_ID: Option[String] = None, // "146"
    COUNTRY: Option[String] = None, // "GB"
    CURRENCY: Option[String] = None, // "GBP"
    DOB: Option[String] = None, // "1966-09-05"
    EMAIL: Option[String] = None, // "Stevehill025@gmail.com"
    FIRST_CASINO_BET: Option[String] = None, //
    FIRST_DEP_DATE: Option[String] = None, //
    FIRST_NAME: Option[String] = None, // "Stephen"
    FIRST_SPORTS_BET: Option[String] = None, // "Stephen"
    IS_SELF_EXCLUDED: Option[String] = None, // "0"
    LAST_NAME: Option[String] = None, // "Hill"
    MARKETING_EMAIL: Option[String] = None, // "False"
    MARKETING_PHONE: Option[String] = None, // "False"
    MARKETING_SMS: Option[String] = None, // "True"
    PHONENUMBER: Option[String] = None, // "7523120381"
    PLAYER_ID: Option[String] = None, // "22767"
    REG_DATETIME: Option[String] = None, // "2023-08-12 20:24:31.073"
    STATUS: Option[String] = None, // "Active"
    STATUSDESCRIPTION: Option[String] = None, // "Active"
    STG_HASH: Option[String] = None, // "Active"
    TEST_USER: Option[String] = None, // "0"
    UNIVERSE: Option[String] = None, // "bresbet"
    VIP: Option[String] = None, // "False"
    CITY: Option[String] = None,
    PROMO_ID: Option[String] = None,
    PROMO_BANNED: Option[String] = None,
    POSTCODE: Option[String] = None
)

case class PlayerStatusDB(
    BRAND_ID: Option[String] = None, // "146"
    PLAYER_ID: Option[String] = None, // "22767"
    PLAYER_STATUS_ID: Option[String] = None,
    PLAYER_STATUS_DESCRIPTION: Option[String] = None,
    IS_PLAYBREAK: Option[String] = None,
    PLAYBREAK_EXPIRY: Option[String] = None,
    IS_SELF_EXCLUDED: Option[String] = None
)
