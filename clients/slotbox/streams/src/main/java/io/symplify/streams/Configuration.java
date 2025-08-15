package io.symplify.streams;

public class Configuration {
  public class Store {
    public static final String PLAYERS = "players";
  }

  public class Topic {
    public static final String REPARTITIONED_PLAYERS = "repartitioned_customer_detail";
    public static final String REPARTITIONED_PLAYER_STATUS = "repartitioned_player_status";
    public static final String REPARTITIONED_PLAYER_CONSENT = "repartitioned_player_consent";
    public static final String REPARTITIONED_WALLET = "repartitioned_wallet";

    public static final String PLAYERS = "customer_detail";
    public static final String PLAYER_CONSENT = "player_consent";
    public static final String PLAYER_STATUS = "player_status";
    public static final String LOGINS = "logins";
    public static final String WAGERING = "wagering";
    public static final String WALLET = "wallet";
  }

  public class Mapping {
    public class Type {
      public static final String GENERIC_USER = "GENERIC_USER";
      public static final String USER_BLOCK = "USER_BLOCK";
      public static final String USER_CONSENT_UPDATE = "USER_CONSENT_UPDATE";
    }

    public class Selector {
      public static final String PLAYER_REGISTRATION = "player_registration";
      public static final String PLAYER_UPDATED = "player_updated";
      public static final String PLAYER_LOGIN = "player_login";
      public static final String FAILED_WITHDRAWAL = "failed_withdrawal";
      public static final String FAILED_DEPOSIT = "failed_deposit";
      public static final String FIRST_DEPOSIT = "first_deposit";
      public static final String BIG_WIN = "big_win";
      public static final String PLAYER_BLOCKED = "player_blocked";
      public static final String SELF_EXCLUSION = "self_exclusion";
      public static final String PLAYER_CONSENT = "player_consent";
      public static final String DEPOSIT_APPROVED = "DEPOSIT_APPROVED";
      public static final String WITHDRAWAL_APPROVED = "WITHDRAWAL_APPROVED";
    }
  }

}
