package io.symplify.sqs;

import java.util.Objects;
import java.util.Optional;

public class PlayerSqs {

  public Optional<String> originalId = Optional.empty();
  public Optional<String> player_id = Optional.empty();
  public Optional<String> reg_datetime = Optional.empty();
  public Optional<String> first_name = Optional.empty();
  public Optional<String> last_name = Optional.empty();
  public Optional<String> email = Optional.empty();
  public Optional<String> phone_number = Optional.empty();
  public Optional<String> language = Optional.empty();
  public Optional<String> affiliate_id = Optional.empty();
  public Optional<String> is_self_excluded = Optional.empty();
  public Optional<String> first_dep_datetime = Optional.empty();
  public Optional<String> dob = Optional.empty();
  public Optional<String> country_id = Optional.empty(); // weird because the currency does not have _id
  public Optional<String> vip = Optional.empty();
  public Optional<String> test_user = Optional.empty();
  public Optional<String> currency = Optional.empty();
  public Optional<String> brand_id = Optional.empty();

  public Optional<String> sex = Optional.empty();
  public Optional<String> region = Optional.empty();
  public Optional<String> accountName = Optional.empty();
  public Optional<String> accountStatus = Optional.empty();
  public Optional<String> bankidValidated = Optional.empty();
  public Optional<String> locked = Optional.empty();
  public Optional<String> leoJackpot = Optional.empty();
  public Optional<String> migratedFrom = Optional.empty();
  public Optional<String> seonEmailStatus = Optional.empty();
  public Optional<String> verifiedUntil = Optional.empty();
  public Optional<String> verticalSportsOnly = Optional.empty();
  public Optional<String> welcomeOffer = Optional.empty();
  public Optional<String> withdrawalPending = Optional.empty();
  public Optional<String> permissionReceiveBingoPromotions = Optional.empty();
  public Optional<String> permissionReceiveCasinoPromotions = Optional.empty();
  public Optional<String> permissionReceiveLiveCasinoPromotions = Optional.empty();
  public Optional<String> permissionReceiveSportsCasinoPromotions = Optional.empty();
  public Optional<String> wantscommunication = Optional.empty();
  public Optional<String> wantssms = Optional.empty();
  public Optional<String> wantscalls = Optional.empty();
  public Optional<String> wantsapppush = Optional.empty();

  public PlayerSqs() {
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    PlayerSqs other = (PlayerSqs) obj;

    return Objects.equals(this.originalId, other.originalId) &&
        Objects.equals(this.player_id, other.player_id) &&
        Objects.equals(this.is_self_excluded, other.is_self_excluded) &&
        Objects.equals(this.reg_datetime, other.reg_datetime) &&
        Objects.equals(this.first_name, other.first_name) &&
        Objects.equals(this.last_name, other.last_name) &&
        Objects.equals(this.email, other.email) &&
        Objects.equals(this.phone_number, other.phone_number) &&
        Objects.equals(this.language, other.language) &&
        Objects.equals(this.affiliate_id, other.affiliate_id) &&
        Objects.equals(this.first_dep_datetime, other.first_dep_datetime) &&
        Objects.equals(this.dob, other.dob) &&
        Objects.equals(this.country_id, other.country_id) &&
        Objects.equals(this.vip, other.vip) &&
        Objects.equals(this.test_user, other.test_user) &&
        Objects.equals(this.currency, other.currency) &&
        Objects.equals(this.brand_id, other.brand_id) &&
        Objects.equals(this.sex, other.sex) &&
        Objects.equals(this.region, other.region) &&
        Objects.equals(this.accountName, other.accountName) &&
        Objects.equals(this.accountStatus, other.accountStatus) &&
        Objects.equals(this.bankidValidated, other.bankidValidated) &&
        Objects.equals(this.locked, other.locked) &&
        Objects.equals(this.leoJackpot, other.leoJackpot) &&
        Objects.equals(this.migratedFrom, other.migratedFrom) &&
        Objects.equals(this.seonEmailStatus, other.seonEmailStatus) &&
        Objects.equals(this.verifiedUntil, other.verifiedUntil) &&
        Objects.equals(this.verticalSportsOnly, other.verticalSportsOnly) &&
        Objects.equals(this.welcomeOffer, other.welcomeOffer) &&
        Objects.equals(this.withdrawalPending, other.withdrawalPending) &&
        Objects.equals(this.permissionReceiveBingoPromotions, other.permissionReceiveBingoPromotions) &&
        Objects.equals(this.permissionReceiveCasinoPromotions, other.permissionReceiveCasinoPromotions) &&
        Objects.equals(this.permissionReceiveLiveCasinoPromotions, other.permissionReceiveLiveCasinoPromotions) &&
        Objects.equals(this.permissionReceiveSportsCasinoPromotions, other.permissionReceiveSportsCasinoPromotions) &&
        Objects.equals(this.wantscommunication, other.wantscommunication) &&
        Objects.equals(this.wantssms, other.wantssms) &&
        Objects.equals(this.wantscalls, other.wantscalls) &&
        Objects.equals(this.wantsapppush, other.wantsapppush);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.originalId,
        this.player_id,
        this.is_self_excluded,
        this.reg_datetime,
        this.first_name,
        this.last_name,
        this.email,
        this.phone_number,
        this.language,
        this.affiliate_id,
        this.first_dep_datetime,
        this.dob,
        this.country_id,
        this.vip,
        this.test_user,
        this.currency,
        this.brand_id,
        this.sex,
        this.region,
        this.accountName,
        this.accountStatus,
        this.bankidValidated,
        this.locked,
        this.leoJackpot,
        this.migratedFrom,
        this.seonEmailStatus,
        this.verifiedUntil,
        this.verticalSportsOnly,
        this.welcomeOffer,
        this.withdrawalPending,
        this.permissionReceiveBingoPromotions,
        this.permissionReceiveCasinoPromotions,
        this.permissionReceiveLiveCasinoPromotions,
        this.permissionReceiveSportsCasinoPromotions,
        this.wantscommunication,
        this.wantssms,
        this.wantscalls,
        this.wantsapppush);
  }

  public static Optional<PlayerSqs> transform(io.symplify.store.PlayerStore store) {
    return store.player.map(p -> {
      PlayerSqs sqs = new PlayerSqs();
      sqs.originalId = p.player_id;
      sqs.player_id = p.player_id;
      sqs.brand_id = p.brand_id;
      sqs.reg_datetime = p.reg_datetime.map(Transformer::truncateOrKeep);
      sqs.first_name = p.first_name;
      sqs.last_name = p.last_name;
      sqs.email = p.email;
      sqs.phone_number = p.phone_number;
      sqs.language = p.language;
      sqs.affiliate_id = p.affiliate_id;
      sqs.first_dep_datetime = p.first_dep_datetime.map(Transformer::truncateOrKeep);
      sqs.dob = p.dob.map(Transformer::truncateOrKeep);
      sqs.vip = p.vip;
      sqs.test_user = p.test_user;
      sqs.currency = p.currency_id;
      sqs.country_id = p.country_id;
      sqs.is_self_excluded = p.is_self_excluded;
      sqs.sex = p.sex;
      sqs.region = p.region;
      sqs.accountName = p.accountName;
      sqs.accountStatus = p.accountStatus;
      sqs.bankidValidated = p.bankidValidated;
      sqs.locked = p.locked;
      sqs.leoJackpot = p.leoJackpot;
      sqs.migratedFrom = p.migratedFrom;
      sqs.seonEmailStatus = p.seonEmailStatus;
      sqs.verifiedUntil = p.verifiedUntil;
      sqs.verticalSportsOnly = p.verticalSportsOnly;
      sqs.welcomeOffer = p.welcomeOffer;
      sqs.withdrawalPending = p.withdrawalPending;
      sqs.permissionReceiveBingoPromotions = p.permissionReceiveBingoPromotions;
      sqs.permissionReceiveCasinoPromotions = p.permissionReceiveCasinoPromotions;
      sqs.permissionReceiveLiveCasinoPromotions = p.permissionReceiveLiveCasinoPromotions;
      sqs.permissionReceiveSportsCasinoPromotions = p.permissionReceiveSportsCasinoPromotions;
      sqs.wantscommunication = p.wantscommunication;
      sqs.wantssms = p.wantssms;
      sqs.wantscalls = p.wantscalls;
      sqs.wantsapppush = p.wantsapppush;
      return sqs;
    });
  }
}
