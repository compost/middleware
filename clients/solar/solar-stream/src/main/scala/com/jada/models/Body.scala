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
