package io.symplify.streams;

public class Configuration {
  public class Store {
    public static final String PLAYERS = "players_new";
  }

  public class Topic {
    public static final String PLAYERS = "players";
    public static final String PLAYER_STATUS = "player_status";
    public static final String LOGINS = "logins";
    public static final String WAGERING = "wagering";
    public static final String WALLET = "wallet";
    public static final String TRANSACTIONS = "transactions";
    public static final String USER_CONSENT_UPDATE = "user_consent_update";
  }

  public class Mapping {
    public class Type {
      public static final String GENERIC_USER = "GENERIC_USER";
      public static final String USER_BLOCKED_TOGGLE = "USER_BLOCKED_TOGGLE";
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
      public static final String SUCCESS_DEPOSIT = "success_deposit";
      public static final String SUCCESS_WITHDRAWAL = "success_withdrawal";
      public static final String PLAYER_PURGE = "player_purge";
      public static final String CONSENT_CHANGE = "consent_change";
    }
  }

}
