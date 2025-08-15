package com.leovegas.model

case class DataBet(
    sportsbet: Option[SportsBets] = None,
    wagering: Option[Wagering] = None,
    processed: Boolean = false,
    init: Option[Boolean] = Some(false)
)

object DataBet {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val sportsBetsEncoder: Encoder[SportsBets] = deriveEncoder
  implicit val wageringEncoder: Encoder[Wagering] = deriveEncoder
  implicit val dataBetEncoder: Encoder[DataBet] = deriveEncoder

}
