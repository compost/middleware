package com.jada.models

import io.circe.Encoder
import io.circe.generic.semiauto._

case class Body[T](
    `type`: String,
    contactId: String,
    mappingSelector: String,
    properties: T
)

object Body {
  implicit def bodyEncoder[T: Encoder]: Encoder[Body[T]] = deriveEncoder
}

case class BodyBlocks(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    properties: CustomerBlocks,
    blocked: Boolean
)

object BodyBlocks {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val customerBlocksEncoder: Encoder[CustomerBlocks] =
    deriveEncoder

  implicit val bodyBlocksEncoder: Encoder[BodyBlocks] = deriveEncoder
}

case class BodyConsent(
    `type`: String,
    contactId: String,
    mappingSelector: String,
    consent: CustomerConsent,
)

object BodyConsent {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val customerConsentEncoder: Encoder[CustomerConsent] =
    deriveEncoder

  implicit val bodyConsentEncoder: Encoder[BodyConsent] = deriveEncoder
}
