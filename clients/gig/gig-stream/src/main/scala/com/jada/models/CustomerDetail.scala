package com.jada.models

import com.jada.models.keepYYYYMMDD

object CustomerDetail {
  val CustomerCreated = "Customer Created"
  val CustomerUpdated = "Customer Updated"
  val CustomerVerified = "Customer Verified"
  val CustomerTag = "Customer Tag"
  val CustomerSession = "Customer Session"
  val CustomerBlocks = "Customer Blocks"
  val CustomerFPP = "Customer Fpp"
  val CustomerConsent = "Customer Consent"
  val CustomerBalance = "Customer Balance"

  import io.circe.Encoder
  import io.circe.generic.semiauto._
  implicit val customerDetailEncoder: Encoder[CustomerDetail] =
    deriveEncoder
}
case class CustomerDetail(
    // CUSTOMER_CREATED CUSTOMER_UPDATED
    activity_field: Option[String] = None,
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    last_name: Option[String] = None,
    current_country_name: Option[String] = None,
    acquisition_source_code: Option[String] = None,
    city: Option[String] = None,
    mobile: Option[String] = None,
    sex: Option[String] = None,
    birth_date: Option[String] = None,
    signup_language: Option[String] = None,
    postal_code: Option[String] = None,
    first_name: Option[String] = None,
    email: Option[String] = None,
    account_creation_datetime_local: Option[String] = None,
    phone: Option[String] = None,
    customer_currency_code: Option[String] = None,
    username: Option[String] = None,
    role_name: Option[String] = None,

    // CUSTOMER_VERIFIED
    account_verification_datetime_local: Option[String] = None,
    reference_btag: Option[String] = None,
    click_id: Option[String] = None,

    // CUSTOMER_TAG
    tag_name: Option[String] = None,
    is_abuser: Option[String] = None,
    // CUSTOMER_SESSION
    last_login_datetime_local: Option[String] = None,
    last_deposit_datetime_local: Option[String] = None,
    // CUSTOMER_BLOCKS
    is_blocked: Option[String] = None,
    is_self_excluded: Option[String] = None,

    // first
    first_deposit_datetime: Option[String] = None,
    first_withdrawal_datetime: Option[String] = None,
    // FPP
    fpp_customer_level: Option[String] = None,
    fpp_reward_level_id: Option[String] = None,
    fpp_update_datetime_local: Option[String] = None,
    // Consent
    consent_marketing_email: Option[String] = None,
    consent_marketing_text_message: Option[String] = None,
    consent_marketing_direct_mail: Option[String] = None,
    consent_marketing_telephone: Option[String] = None,
    consent_marketing_oms: Option[String] = None,

    // Balance
    wallet_balance_locked_money_tenant: Option[String] = None,
    wallet_balance_total_money_tenant: Option[String] = None,
    wallet_balance_bonus_money_tenant: Option[String] = None,
    wallet_balance_real_money_tenant: Option[String] = None
)

case class CustomerCreated(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    lastName: Option[String] = None,
    country: Option[String] = None,
    acquisition_source_code: Option[String] = None,
    city: Option[String] = None,
    mobileNumber: Option[String] = None,
    sex: Option[String] = None,
    dateOfBirth: Option[String] = None,
    signup_language: Option[String] = None,
    zipcode: Option[String] = None,
    firstName: Option[String] = None,
    emailAddress: Option[String] = None,
    account_creation_date: Option[String] = None,
    phone: Option[String] = None,
    customer_currency_code: Option[String] = None,
    username: Option[String] = None,
    player_roles: Option[String] = None
)

object CustomerCreated {
  def apply(cd: CustomerDetail): CustomerCreated = {
    CustomerCreated(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      lastName = cd.last_name,
      country = cd.current_country_name,
      acquisition_source_code = cd.acquisition_source_code,
      city = cd.city,
      mobileNumber = cd.mobile,
      sex = cd.sex,
      dateOfBirth = cd.birth_date,
      signup_language = cd.signup_language,
      zipcode = cd.postal_code,
      firstName = cd.first_name,
      emailAddress = cd.email,
      account_creation_date =
        cd.account_creation_datetime_local.map(keepYYYYMMDD(_)),
      phone = cd.phone,
      customer_currency_code = cd.customer_currency_code,
      username = cd.username,
      player_roles = cd.role_name
    )
  }
}
case class CustomerUpdated(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    lastName: Option[String] = None,
    country: Option[String] = None,
    acquisition_source_code: Option[String] = None,
    city: Option[String] = None,
    mobileNumber: Option[String] = None,
    sex: Option[String] = None,
    dateOfBirth: Option[String] = None,
    signup_language: Option[String] = None,
    zipcode: Option[String] = None,
    firstName: Option[String] = None,
    emailAddress: Option[String] = None,
    account_creation_date: Option[String] = None,
    phone: Option[String] = None,
    customer_currency_code: Option[String] = None,
    username: Option[String] = None,
    player_roles: Option[String] = None
)

object CustomerUpdated {
  def apply(cd: CustomerDetail): CustomerUpdated = {
    CustomerUpdated(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      lastName = cd.last_name,
      country = cd.current_country_name,
      acquisition_source_code = cd.acquisition_source_code,
      city = cd.city,
      mobileNumber = cd.mobile,
      sex = cd.sex,
      dateOfBirth = cd.birth_date,
      signup_language = cd.signup_language,
      zipcode = cd.postal_code,
      firstName = cd.first_name,
      emailAddress = cd.email,
      account_creation_date =
        cd.account_creation_datetime_local.map(keepYYYYMMDD(_)),
      phone = cd.phone,
      customer_currency_code = cd.customer_currency_code,
      username = cd.username,
      player_roles = cd.role_name
    )
  }
}
case class CustomerVerified(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    lastName: Option[String] = None,
    country: Option[String] = None,
    acquisition_source_code: Option[String] = None,
    city: Option[String] = None,
    mobileNumber: Option[String] = None,
    sex: Option[String] = None,
    dateOfBirth: Option[String] = None,
    signup_language: Option[String] = None,
    zipcode: Option[String] = None,
    firstName: Option[String] = None,
    emailAddress: Option[String] = None,
    account_creation_date: Option[String] = None,
    phone: Option[String] = None,
    customer_currency_code: Option[String] = None,
    username: Option[String] = None,
    account_verification_date: Option[String] = None,
    reference: Option[String] = None,
    click_id: Option[String] = None,
    player_roles: Option[String] = None
)

object CustomerVerified {
  def apply(cd: CustomerDetail): CustomerVerified = {
    CustomerVerified(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      lastName = cd.last_name,
      country = cd.current_country_name,
      acquisition_source_code = cd.acquisition_source_code,
      city = cd.city,
      mobileNumber = cd.mobile,
      sex = cd.sex,
      dateOfBirth = cd.birth_date,
      signup_language = cd.signup_language,
      zipcode = cd.postal_code,
      firstName = cd.first_name,
      emailAddress = cd.email,
      account_creation_date =
        cd.account_creation_datetime_local.map(keepYYYYMMDD(_)),
      phone = cd.phone,
      customer_currency_code = cd.customer_currency_code,
      username = cd.username,
      account_verification_date =
        cd.account_verification_datetime_local.map(keepYYYYMMDD(_)),
      reference = cd.reference_btag,
      click_id = cd.click_id,
      player_roles = cd.role_name
    )
  }
}
case class CustomerTag(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    player_tags: Option[String] = None,
    is_abuser: Option[String] = None
)

object CustomerTag {
  def apply(cd: CustomerDetail): CustomerTag = {
    CustomerTag(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      player_tags = cd.tag_name,
      is_abuser = cd.is_abuser
    )
  }
}

case class CustomerBlocks(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    is_blocked: Option[String] = None,
    last_login_date: Option[String] = None,
    is_self_excluded: Option[String] = None,
    last_deposit_date: Option[String] = None
)

object CustomerBlocks {
  def apply(cd: CustomerDetail): CustomerBlocks = {
    CustomerBlocks(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      is_blocked = cd.is_blocked,
      last_login_date = cd.last_login_datetime_local.map(keepYYYYMMDD(_)),
      is_self_excluded = cd.is_self_excluded,
      last_deposit_date = cd.last_deposit_datetime_local.map(keepYYYYMMDD(_))
    )
  }

  def apply(cd: HistoryBlocked): CustomerBlocks = {
    CustomerBlocks(
      original_id = cd.PLAYER_ID,
      brand_id = cd.BRAND_ID,
      is_blocked = cd.BLOCKED
    )
  }
}

case class CustomerSession(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    last_login_date: Option[String] = None,
    last_deposit_date: Option[String] = None
)

object CustomerSession {
  def apply(cd: CustomerDetail): CustomerSession = {
    CustomerSession(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      last_login_date = cd.last_login_datetime_local.map(keepYYYYMMDD(_)),
      last_deposit_date = cd.last_deposit_datetime_local.map(keepYYYYMMDD(_))
    )
  }
}

case class CustomerFPP(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    FPP_CUSTOMER_LEVEL: Option[String] = None,
    FPP_REWARD_LEVEL_ID: Option[String] = None,
    FPP_UPDATE_DATETIME: Option[String] = None
)

object CustomerFPP {
  def apply(cd: CustomerDetail): CustomerFPP = {
    CustomerFPP(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      FPP_CUSTOMER_LEVEL = cd.fpp_customer_level,
      FPP_REWARD_LEVEL_ID = cd.fpp_reward_level_id,
      FPP_UPDATE_DATETIME = cd.fpp_update_datetime_local
    )
  }
}

case class CustomerConsent(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    consent_marketingemail: Option[String] = None,
    consent_marketingtextmessage: Option[String] = None,
    consent_marketingdirectmail: Option[String] = None,
    consent_marketingtelephone: Option[String] = None,
    consent_marketingoms: Option[String] = None
)

object CustomerConsent {
  def apply(cd: CustomerDetail): CustomerConsent = {
    CustomerConsent(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      consent_marketingemail = cd.consent_marketing_email,
      consent_marketingtextmessage = cd.consent_marketing_text_message,
      consent_marketingdirectmail = cd.consent_marketing_direct_mail,
      consent_marketingtelephone = cd.consent_marketing_telephone,
      consent_marketingoms = cd.consent_marketing_oms
    )
  }

  def apply(cd: HistoryConsent): CustomerConsent = {
    CustomerConsent(
      original_id = cd.PLAYER_ID,
      brand_id = cd.BRAND_ID,
      consent_marketingemail = cd.CONSENT_MARKETING_EMAIL,
      consent_marketingtextmessage = cd.CONSENT_MARKETING_TEXT_MESSAGE,
      consent_marketingdirectmail = cd.CONSENT_MARKETING_DIRECT_MAIL,
      consent_marketingtelephone = cd.CONSENT_MARKETING_TELEPHONE,
      consent_marketingoms = cd.CONSENT_MARKETING_OMS
    )
  }
}

case class CustomerBalance(
    original_id: Option[String] = None,
    brand_id: Option[String] = None,
    locked_money_balance: Option[String] = None,
    wallet_balance_total_money: Option[String] = None,
    bonus_money_balance: Option[String] = None,
    real_money_balance: Option[String] = None
)

object CustomerBalance {
  def apply(cd: CustomerDetail): CustomerBalance = {
    CustomerBalance(
      original_id = cd.player_id,
      brand_id = cd.brand_id,
      locked_money_balance = cd.wallet_balance_locked_money_tenant,
      wallet_balance_total_money = cd.wallet_balance_total_money_tenant,
      bonus_money_balance = cd.wallet_balance_bonus_money_tenant,
      real_money_balance = cd.wallet_balance_real_money_tenant
    )
  }
}
