package com.soft2bet.model

case class Wallet(
    player_id: Option[String],
    transaction_id: Option[String],
    transaction_type_id: Option[String],
    transaction_status_id: Option[String],
    amount: Option[String],
    transaction_datetime: Option[String],
    brand_id: Option[String],
    payment_method: Option[String],
    currency_id: Option[String],
    first_deposit_datetime: Option[String],
    last_deposit_datetime: Option[String],
    CasinoBonusBalance: Option[String],
    SportBonusBalance: Option[String],
    CasinoBonusBalanceEUR: Option[String],
    SportBonusBalanceEUR: Option[String],
    LifetimeWithdrawalCount: Option[String],
    LifetimeWithdrawalCountEUR: Option[String],
    LifetimeDepositCount: Option[String],
    TotalWithdrawalLifetime: Option[String],
    TotalWithdrawalLifetimeEUR: Option[String],
    TotalDeposit: Option[String],
    TotalDepositEUR: Option[String],
    currency: Option[String],
    PayMethod: Option[String]
)

case class BonusUpdatedSQS(
    CasinoBonusBalance: Option[String] = None,
    SportBonusBalance: Option[String] = None,
    CasinoBonusBalanceEUR: Option[String] = None,
    SportBonusBalanceEUR: Option[String] = None,
    LifetimeWithdrawalCount: Option[String] = None,
    TotalWithdrawalLifetime: Option[String] = None,
    TotalWithdrawalLifetimeEUR: Option[String] = None
)

object BonusUpdatedSQS {

  import io.circe.Encoder
  import io.circe.generic.semiauto._

  implicit val bonusWalletSQSEncoder: Encoder[BonusUpdatedSQS] = deriveEncoder

  def apply(w: Wallet): BonusUpdatedSQS = {
    new BonusUpdatedSQS(
      CasinoBonusBalance = w.CasinoBonusBalance,
      SportBonusBalance = w.SportBonusBalance,
      CasinoBonusBalanceEUR = w.CasinoBonusBalanceEUR,
      SportBonusBalanceEUR = w.SportBonusBalanceEUR,
      LifetimeWithdrawalCount = w.LifetimeWithdrawalCount,
      TotalWithdrawalLifetime = w.TotalWithdrawalLifetime,
      TotalWithdrawalLifetimeEUR = w.TotalWithdrawalLifetimeEUR
    )
  }
}

case class WithdrawalRequestSQS(
    playerid: Option[String],
    brandId: Option[String],
    Currency: Option[String],
    PayMethod: Option[String],
    TransactionId: Option[String],
    TransactionDate: Option[String],
    TransactionType: Option[String],
    TransactionAmount: Option[String],
    TransactionStatus: Option[String]
)

object WithdrawalRequestSQS {
  import io.circe.Encoder
  import io.circe.generic.semiauto._

  def apply(
      wallet: Wallet
  ): WithdrawalRequestSQS = {
    WithdrawalRequestSQS(
      playerid = wallet.player_id,
      brandId = wallet.brand_id,
      Currency = wallet.currency,
      PayMethod = wallet.payment_method,
      TransactionId = wallet.transaction_id,
      TransactionDate = wallet.transaction_datetime.map(v =>
        if (v.length > 10) v.substring(0, 10) else v
      ),
      TransactionType = wallet.transaction_type_id,
      TransactionAmount = wallet.amount,
      TransactionStatus = wallet.transaction_status_id
    )
  }
  implicit val withdrawalRequestSQSEncoder: Encoder[WithdrawalRequestSQS] =
    deriveEncoder
}
