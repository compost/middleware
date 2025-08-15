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
    stored.brand_id = player.brand_id.or(() -> stored.brand_id);
    stored.reg_datetime = player.reg_datetime.or(() -> stored.reg_datetime);
    stored.first_name = player.first_name.or(() -> stored.first_name);
    stored.last_name = player.last_name.or(() -> stored.last_name);
    stored.email = player.email.or(() -> stored.email);
    stored.phone_number = player.phone_number.or(() -> stored.phone_number);
    stored.language = player.language.or(() -> stored.language);
    stored.affiliate_id = player.affiliate_id.or(() -> stored.affiliate_id);
    stored.first_dep_datetime = player.first_dep_datetime.or(() -> stored.first_dep_datetime);
    stored.dob = player.dob.or(() -> stored.dob);
    stored.vip = player.vip.or(() -> stored.vip);
    stored.test_user = player.test_user.or(() -> stored.test_user);
    stored.currency_description = player.currency_description.or(() -> stored.currency_description);
    stored.country_description = player.country_description.or(() -> stored.country_description);
    stored.is_self_excluded = player.is_self_excluded.or(() -> stored.is_self_excluded);

    stored.country_id = player.country_id.or(() -> stored.country_id);
    stored.currency_id = player.currency_id.or(() -> stored.currency_id);
    return Optional.of(this);
  }

}
