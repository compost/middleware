package com.soft2bet.model

case class Wagering(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    Balance: Option[String] = None,
    BalanceEUR: Option[String] = None,
    LifetimeDepositCount: Option[String] = None,
    ts: Option[String] = None,
    bet_datetime: Option[String] = None,
    Sent: Option[Boolean] = None
)

case class WageringJSON(
    Balance: Option[String] = None,
    BalanceEUR: Option[String] = None
)

object WageringJSON {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  def apply(w: Wagering): WageringJSON = {
    WageringJSON(Balance = w.Balance, BalanceEUR = w.BalanceEUR)
  }
  implicit val wageringSQSEncoder: Encoder[WageringJSON] = deriveEncoder
}
