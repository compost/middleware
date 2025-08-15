package com.jada.models

case class PlayerExtended(
    originalId: Option[String] = None,
    country_id: Option[String] = None,
    country: Option[String] = None,
    brand_id: Option[String] = None,
    licenseuid: Option[String] = None,
    operator: Option[String] = None
)
