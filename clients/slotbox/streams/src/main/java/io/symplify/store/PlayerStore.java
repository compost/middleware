package io.symplify.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.symplify.kafka.PlayerStatusKafka;

@RegisterForReflection
public class PlayerStore {

  public Optional<io.symplify.kafka.PlayerKafka> player = Optional.empty();
  public Optional<io.symplify.kafka.PlayerStatusKafka> status = Optional.empty();
  public Map<String, io.symplify.kafka.PlayerConsentKafka> playerConsents = new HashMap<String, io.symplify.kafka.PlayerConsentKafka>();
  public Optional<Boolean> playerRegistrationHasBeenSent = Optional.empty();

  public PlayerStore withPlayerRegistrationHasBeenSent(Boolean value) {
    playerRegistrationHasBeenSent = Optional.ofNullable(value);
    return this;
  }

  public Optional<PlayerStore> with(io.symplify.kafka.PlayerConsentKafka playerConsent) {
    if (playerConsent.channel.isEmpty()) {
      return Optional.empty();
    }
    var channel = playerConsent.channel.get();
    var previous = this.playerConsents.get(channel);
    if (previous != null && previous.consented.equals(playerConsent.consented)) {
      return Optional.empty();
    }
    this.playerConsents.put(channel, playerConsent);
    return Optional.of(this);
  }

  public Optional<PlayerStore> with(PlayerStatusKafka kafka) {
    this.status = Optional.ofNullable(kafka);
    return Optional.of(this);
  }

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
    stored.customer_id = player.customer_id.filter(v -> v != "").or(() -> stored.customer_id);
    stored.external_player_id = player.external_player_id.filter(v -> v != "").or(() -> stored.external_player_id);
    stored.unique_id = player.unique_id.filter(v -> v != "").or(() -> stored.unique_id);
    stored.first_name = player.first_name.filter(v -> v != "").or(() -> stored.first_name);
    stored.last_name = player.last_name.filter(v -> v != "").or(() -> stored.last_name);
    stored.middlename = player.middlename.filter(v -> v != "").or(() -> stored.middlename);
    stored.username = player.username.filter(v -> v != "").or(() -> stored.username);
    stored.title = player.title.filter(v -> v != "").or(() -> stored.title);
    stored.birth_date = player.birth_date.filter(v -> v != "").or(() -> stored.birth_date);
    stored.sex = player.sex.filter(v -> v != "").or(() -> stored.sex);
    stored.country_alpha_2_code = player.country_alpha_2_code.filter(v -> v != "")
        .or(() -> stored.country_alpha_2_code);
    stored.current_country_name = player.current_country_name.filter(v -> v != "")
        .or(() -> stored.current_country_name);
    stored.city = player.city.filter(v -> v != "").or(() -> stored.city);
    stored.zipcode = player.zipcode.filter(v -> v != "").or(() -> stored.zipcode);
    stored.address1 = player.address1.filter(v -> v != "").or(() -> stored.address1);
    stored.address2 = player.address2.filter(v -> v != "").or(() -> stored.address2);
    stored.language_alpha_2_code = player.language_alpha_2_code.filter(v -> v != "")
        .or(() -> stored.language_alpha_2_code);
    stored.signup_language = player.signup_language.filter(v -> v != "").or(() -> stored.signup_language);
    stored.currency_code = player.currency_code.filter(v -> v != "").or(() -> stored.currency_code);
    stored.is_blocked = player.is_blocked.filter(v -> v != "").or(() -> stored.is_blocked);
    stored.is_self_excluded = player.is_self_excluded.filter(v -> v != "").or(() -> stored.is_self_excluded);
    stored.account_creation_datetime_local = player.account_creation_datetime_local
        .filter(v -> v != "").or(() -> stored.account_creation_datetime_local);
    stored.account_verification_datetime_local = player.account_verification_datetime_local
        .filter(v -> v != "").or(() -> stored.account_verification_datetime_local);
    stored.email = player.email.filter(v -> v != "").or(() -> stored.email);
    stored.phone_prefix = player.phone_prefix.filter(v -> v != "").or(() -> stored.phone_prefix);
    stored.phone = player.phone.filter(v -> v != "").or(() -> stored.phone);
    stored.mobile_phone_prefix = player.mobile_phone_prefix.filter(v -> v != "").or(() -> stored.mobile_phone_prefix);
    stored.mobile_phone = player.mobile_phone.filter(v -> v != "").or(() -> stored.mobile_phone);
    stored.mobile = player.mobile.filter(v -> v != "").or(() -> stored.mobile);
    stored.acquisition_source_code = player.acquisition_source_code.filter(v -> v != "")
        .or(() -> stored.acquisition_source_code);
    stored.reference = player.reference.filter(v -> v != "").or(() -> stored.reference);
    stored.role_name = player.role_name.filter(v -> v != "").or(() -> stored.role_name);
    stored.tag_name = player.tag_name.filter(v -> v != "").or(() -> stored.tag_name);
    stored.is_abuser = player.is_abuser.filter(v -> v != "").or(() -> stored.is_abuser);
    stored.fpp_reward_level_name = player.fpp_reward_level_name.filter(v -> v != "")
        .or(() -> stored.fpp_reward_level_name);
    stored.fpp_customer_level = player.fpp_customer_level.filter(v -> v != "").or(() -> stored.fpp_customer_level);
    stored.fpp_update_datetime_local = player.fpp_update_datetime_local.filter(v -> v != "")
        .or(() -> stored.fpp_update_datetime_local);
    stored.last_login_datetime_local = player.last_login_datetime_local.filter(v -> v != "")
        .or(() -> stored.last_login_datetime_local);
    stored.last_deposit_date = player.last_deposit_date.filter(v -> v != "").or(() -> stored.last_deposit_date);
    stored.last_deposit_tenant = player.last_deposit_tenant.filter(v -> v != "").or(() -> stored.last_deposit_tenant);
    stored.last_deposit_customer = player.last_deposit_customer.filter(v -> v != "")
        .or(() -> stored.last_deposit_customer);
    stored.last_withdrawal_tenant = player.last_withdrawal_tenant.filter(v -> v != "")
        .or(() -> stored.last_withdrawal_tenant);
    stored.last_withdrawal_datetime_local = player.last_withdrawal_datetime_local
        .filter(v -> v != "").or(() -> stored.last_withdrawal_datetime_local);
    stored.wallet_balance_total_money_tenant = player.wallet_balance_total_money_tenant
        .filter(v -> v != "").or(() -> stored.wallet_balance_total_money_tenant);
    stored.wallet_balance_real_money_tenant = player.wallet_balance_real_money_tenant
        .filter(v -> v != "").or(() -> stored.wallet_balance_real_money_tenant);
    stored.wallet_balance_bonus_money_tenant = player.wallet_balance_bonus_money_tenant
        .filter(v -> v != "").or(() -> stored.wallet_balance_bonus_money_tenant);
    stored.wallet_balance_locked_money_tenant = player.wallet_balance_locked_money_tenant
        .filter(v -> v != "").or(() -> stored.wallet_balance_locked_money_tenant);
    stored.first_deposit_datetime_local = player.first_deposit_datetime_local
        .filter(v -> v != "").or(() -> stored.first_deposit_datetime_local);
    stored.first_withdrawal_datetime_local = player.first_withdrawal_datetime_local
        .filter(v -> v != "").or(() -> stored.first_withdrawal_datetime_local);
    stored.casino_lifetime_ngr_tenant = player.casino_lifetime_ngr_tenant.filter(v -> v != "")
        .or(() -> stored.casino_lifetime_ngr_tenant);
    stored.casino_lifetime_ggr_tenant = player.casino_lifetime_ggr_tenant.filter(v -> v != "")
        .or(() -> stored.casino_lifetime_ggr_tenant);
    stored.lifetime_deposit_tenant = player.lifetime_deposit_tenant.filter(v -> v != "")
        .or(() -> stored.lifetime_deposit_tenant);
    stored.lifetime_deposit_count = player.lifetime_deposit_count.filter(v -> v != "")
        .or(() -> stored.lifetime_deposit_count);
    stored.casino_lifetime_bet_tenant = player.casino_lifetime_bet_tenant.filter(v -> v != "")
        .or(() -> stored.casino_lifetime_bet_tenant);
    stored.lifetime_withdrawal_tenant = player.lifetime_withdrawal_tenant.filter(v -> v != "")
        .or(() -> stored.lifetime_withdrawal_tenant);
    stored.lifetime_withdrawal_count = player.lifetime_withdrawal_count.filter(v -> v != "")
        .or(() -> stored.lifetime_withdrawal_count);
    stored.sport_lifetime_average_bet_tenant = player.sport_lifetime_average_bet_tenant
        .filter(v -> v != "").or(() -> stored.sport_lifetime_average_bet_tenant);
    stored.casino_lifetime_average_bet_tenant = player.casino_lifetime_average_bet_tenant
        .filter(v -> v != "").or(() -> stored.casino_lifetime_average_bet_tenant);
    stored.lifetime_deposit_bonus_cost_percentage_tenant = player.lifetime_deposit_bonus_cost_percentage_tenant
        .filter(v -> v != "").or(() -> stored.lifetime_deposit_bonus_cost_percentage_tenant);
    stored.sport_lifetime_bet_count = player.sport_lifetime_bet_count.filter(v -> v != "")
        .or(() -> stored.sport_lifetime_bet_count);
    return Optional.of(this);
  }

}
