package com.jada.models

case class Wallet(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    transaction_datetime: Option[String] = None,
    transaction_type_id: Option[String] = None,
    amount: Option[String] = None
)

object Wallet {
  val Deposit = "deposit"
  val Withdrawal = "withdrawal"
}

case class FirstDeposit(
    first_deposit_date: Option[String] = None,
    first_deposit: Option[String] = None
)

object FirstDeposit {
  def apply(w: Wallet): Option[FirstDeposit] = {
    w.transaction_type_id
      .map(_.toLowerCase)
      .filter(_.contains(Wallet.Deposit))
      .map(_ =>
        FirstDeposit(
          first_deposit = w.amount,
          first_deposit_date = w.transaction_datetime.map(keepYYYYMMDD(_))
        )
      )
  }
}

case class LastDeposit(
    last_deposit_date: Option[String] = None,
    last_deposit: Option[String] = None
)

object LastDeposit {
  def apply(w: Wallet): Option[LastDeposit] = {
    w.transaction_type_id
      .map(_.toLowerCase)
      .filter(_.contains(Wallet.Deposit))
      .map(_ =>
        LastDeposit(
          last_deposit = w.amount,
          last_deposit_date = w.transaction_datetime.map(keepYYYYMMDD(_))
        )
      )
  }
}

case class FirstWithdrawal(
    first_withdrawal_date: Option[String] = None,
    first_withdrawal: Option[String] = None
)

object FirstWithdrawal {
  def apply(w: Wallet): Option[FirstWithdrawal] = {
    w.transaction_type_id
      .map(_.toLowerCase)
      .filter(_.contains(Wallet.Withdrawal))
      .map(_ =>
        FirstWithdrawal(
          first_withdrawal = w.amount,
          first_withdrawal_date = w.transaction_datetime.map(keepYYYYMMDD(_))
        )
      )
  }
}

case class LastWithdrawal(
    last_withdrawal_date: Option[String] = None,
    last_withdrawal: Option[String] = None
)

object LastWithdrawal {
  def apply(w: Wallet): Option[LastWithdrawal] = {
    w.transaction_type_id
      .map(_.toLowerCase)
      .filter(_.contains(Wallet.Withdrawal))
      .map(_ =>
        LastWithdrawal(
          last_withdrawal = w.amount,
          last_withdrawal_date = w.transaction_datetime.map(keepYYYYMMDD(_))
        )
      )
  }
}
