package io.symplify.store;

import java.util.Optional;

public class PlayerStore {

  public Optional<io.symplify.kafka.PlayerKafka> player = Optional.empty();

  public Optional<PlayerStore> with(io.symplify.kafka.PlayerKafka player) {
    if (this.player == null) {
      this.player = Optional.ofNullable(player);
      return Optional.of(this);
    }

    if (this.player.isEmpty()) {
      this.player = Optional.ofNullable(player);
      return Optional.of(this);
    }
    var stored = this.player.get();
    stored.player_id = player.player_id.or(() -> stored.player_id);
    stored.reg_datetime = player.reg_datetime.or(() -> stored.reg_datetime);
    stored.first_name = player.first_name.or(() -> stored.first_name);
    stored.last_name = player.last_name.or(() -> stored.last_name);
    stored.email = player.email.or(() -> stored.email);
    stored.phone_number = player.phone_number.or(() -> stored.phone_number);
    stored.language = player.language.or(() -> stored.language);
    stored.affiliate_id = player.affiliate_id.or(() -> stored.affiliate_id);
    stored.is_self_excluded = player.is_self_excluded.or(() -> stored.is_self_excluded);
    stored.first_dep_datetime = player.first_dep_datetime.or(() -> stored.first_dep_datetime);
    stored.dob = player.dob.or(() -> stored.dob);
    stored.country_id = player.country_id.or(() -> stored.country_id);
    stored.country_description = player.country_description.or(() -> stored.country_description); // missing in the
                                                                                                  // mapping sheet
    stored.vip = player.vip.or(() -> stored.vip);
    stored.test_user = player.test_user.or(() -> stored.test_user);
    stored.currency_id = player.currency_id.or(() -> stored.currency_id);
    stored.currency_description = player.currency_description.or(() -> stored.currency_description); // missing in the
                                                                                                     // mapping sheet
    stored.sex = player.sex.or(() -> stored.sex);
    stored.region = player.region.or(() -> stored.region);
    stored.accountName = player.accountName.or(() -> stored.accountName);
    stored.accountStatus = player.accountStatus.or(() -> stored.accountStatus);
    stored.bankidValidated = player.bankidValidated.or(() -> stored.bankidValidated);
    stored.locked = player.locked.or(() -> stored.locked);
    stored.leoJackpot = player.leoJackpot.or(() -> stored.leoJackpot);
    stored.migratedFrom = player.migratedFrom.or(() -> stored.migratedFrom);
    stored.seonEmailStatus = player.seonEmailStatus.or(() -> stored.seonEmailStatus);
    stored.verifiedUntil = player.verifiedUntil.or(() -> stored.verifiedUntil);
    stored.verticalSportsOnly = player.verticalSportsOnly.or(() -> stored.verticalSportsOnly);
    stored.welcomeOffer = player.welcomeOffer.or(() -> stored.welcomeOffer);
    stored.withdrawalPending = player.withdrawalPending.or(() -> stored.withdrawalPending);
    stored.permissionReceiveBingoPromotions = player.permissionReceiveBingoPromotions
        .or(() -> stored.permissionReceiveBingoPromotions);
    stored.permissionReceiveCasinoPromotions = player.permissionReceiveCasinoPromotions
        .or(() -> stored.permissionReceiveCasinoPromotions);
    stored.permissionReceiveLiveCasinoPromotions = player.permissionReceiveLiveCasinoPromotions
        .or(() -> stored.permissionReceiveLiveCasinoPromotions);
    stored.permissionReceiveSportsCasinoPromotions = player.permissionReceiveSportsCasinoPromotions
        .or(() -> stored.permissionReceiveSportsCasinoPromotions);
    stored.wantscommunication = player.wantscommunication.or(() -> stored.wantscommunication);
    stored.wantssms = player.wantssms.or(() -> stored.wantssms);
    stored.wantscalls = player.wantscalls.or(() -> stored.wantscalls);
    stored.wantsapppush = player.wantsapppush.or(() -> stored.wantsapppush);
    stored.wantscalls_casino = player.wantscalls_casino.or(() -> stored.wantscalls_casino);
    stored.wantscalls_livecasino = player.wantscalls_livecasino.or(() -> stored.wantscalls_livecasino);
    stored.wantscalls_sports = player.wantscalls_sports.or(() -> stored.wantscalls_sports);
    stored.wantscalls_bingo = player.wantscalls_bingo.or(() -> stored.wantscalls_bingo);
    stored.wantscommunication_casino = player.wantscommunication_casino.or(() -> stored.wantscommunication_casino);
    stored.wantscommunication_livecasino = player.wantscommunication_livecasino
        .or(() -> stored.wantscommunication_livecasino);
    stored.wantscommunication_sports = player.wantscommunication_sports.or(() -> stored.wantscommunication_sports);
    stored.wantscommunication_bingo = player.wantscommunication_bingo.or(() -> stored.wantscommunication_bingo);
    stored.wantssms_casino = player.wantssms_casino.or(() -> stored.wantssms_casino);
    stored.wantssms_livecasino = player.wantssms_livecasino.or(() -> stored.wantssms_livecasino);
    stored.wantssms_sports = player.wantssms_sports.or(() -> stored.wantssms_sports);
    stored.wantssms_bingo = player.wantssms_bingo.or(() -> stored.wantssms_bingo);
    stored.wantssocialmedia = player.wantssocialmedia.or(() -> stored.wantssocialmedia);
    return Optional.of(this);
  }

}
