package com.jada.models

case class History(
    ORIGINALID: Option[String] = None,
    BRAND_ID: Option[String] = None,
    FIRST_DEPOSIT_DATE: Option[String] = None,
    FIRST_WITHDRAWAL_DATE: Option[String] = None
)

case class HistoryBlocked(
    PLAYER_ID: Option[String] = None,
    BRAND_ID: Option[String] = None,
    BLOCKED: Option[String] = None
)
//{"BRAND":"SpinAway2","BRAND_ID":"192","CONSENT_MARKETING_DIRECT_MAIL":"False","CONSENT_MARKETING_EMAIL":"True","CONSENT_MARKETING_OMS":"False","CONSENT_MARKETING_TELEPHONE":"False","CONSENT_MARKETING_TEXT_MESSAGE":"True","PLAYER_ID":"119583","TENANT_ID":"105"}
case class HistoryConsent(
    PLAYER_ID: Option[String] = None,
    BRAND_ID: Option[String] = None,
    CONSENT_MARKETING_DIRECT_MAIL: Option[String] = None,
    CONSENT_MARKETING_EMAIL: Option[String] = None,
    CONSENT_MARKETING_OMS: Option[String] = None,
    CONSENT_MARKETING_TELEPHONE: Option[String] = None,
    CONSENT_MARKETING_TEXT_MESSAGE: Option[String] = None,
)
