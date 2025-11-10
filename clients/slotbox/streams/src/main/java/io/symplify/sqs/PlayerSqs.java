package io.symplify.sqs;

import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class PlayerSqs {
  public Optional<String> originalid = Optional.empty();
  public Optional<String> external_player_id = Optional.empty();
  public Optional<String> unique_id = Optional.empty();
  public Optional<String> firstname = Optional.empty();
  public Optional<String> lastname = Optional.empty();
  public Optional<String> middlename = Optional.empty();
  public Optional<String> username = Optional.empty();
  public Optional<String> title = Optional.empty();
  public Optional<String> dateofbirth = Optional.empty();
  public Optional<String> sex = Optional.empty();
  public Optional<String> country = Optional.empty();
  public Optional<String> registration_country = Optional.empty();
  public Optional<String> current_country = Optional.empty();
  public Optional<String> city = Optional.empty();
  public Optional<String> zipcode = Optional.empty();
  public Optional<String> address1 = Optional.empty();
  public Optional<String> address2 = Optional.empty();
  public Optional<String> signup_language_ID = Optional.empty();
  public Optional<String> signup_language = Optional.empty();
  public Optional<String> currency_code = Optional.empty();
  public Optional<String> is_blocked = Optional.empty();
  public Optional<String> is_self_excluded = Optional.empty();
  public Optional<String> account_creation_date = Optional.empty();
  public Optional<String> account_verification_date = Optional.empty();
  public Optional<String> emailaddress = Optional.empty();
  public Optional<String> phone_prefix = Optional.empty();
  public Optional<String> phone = Optional.empty();
  public Optional<String> mobile_phone_prefix = Optional.empty();
  public Optional<String> mobile_phone = Optional.empty();
  public Optional<String> mobilenumber = Optional.empty();
  public Optional<String> acquisition = Optional.empty();
  public Optional<String> reference = Optional.empty();
  public Optional<String> player_roles = Optional.empty();
  public Optional<String> player_tags = Optional.empty();
  public Optional<String> abuser_role_tag = Optional.empty();
  public Optional<String> reward_indicator = Optional.empty();
  public Optional<String> fpp_level = Optional.empty();
  public Optional<String> fpp_reward_level = Optional.empty();
  public Optional<String> fpp_reward_level_date_awarded = Optional.empty();
  public Optional<String> last_login_date = Optional.empty();
  public Optional<String> last_deposit = Optional.empty();
  public Optional<String> last_deposit_customer_currency = Optional.empty();
  public Optional<String> last_deposit_date = Optional.empty();
  public Optional<String> last_withdrawal = Optional.empty();
  public Optional<String> last_withdrawal_date = Optional.empty();
  public Optional<String> total_balance = Optional.empty();
  public Optional<String> real_money_balance = Optional.empty();
  public Optional<String> bonus_money_balance = Optional.empty();
  public Optional<String> locked_money_balance = Optional.empty();
  public Optional<String> first_deposit_date = Optional.empty();
  public Optional<String> first_withdrawal_date = Optional.empty();
  public Optional<String> lifetime_ngr = Optional.empty();
  public Optional<String> lifetime_ggr = Optional.empty();
  public Optional<String> lifetime_deposits = Optional.empty();
  public Optional<String> lifetime_deposits_count = Optional.empty();
  public Optional<String> lifetime_turnover = Optional.empty();
  public Optional<String> lifetime_withdrawals = Optional.empty();
  public Optional<String> lifetime_withdrawals_count = Optional.empty();
  public Optional<String> sport_lifetime_average_bet = Optional.empty();
  public Optional<String> casino_lifetime_average_bet = Optional.empty();
  public Optional<String> casino_lifetime_real_money_ngr = Optional.empty();
  public Optional<String> lifetime_deposits_over_lifetime_bonus_cost = Optional.empty();
  public Optional<String> lifetime_sports_bet_count = Optional.empty();
  public Optional<String> consent_marketingtelephone = Optional.empty();
  public Optional<String> consent_marketingdirectmail = Optional.empty();
  public Optional<String> consent_marketingoms = Optional.empty();

  public PlayerSqs() {
  }

  public static Optional<PlayerSqs> transform(io.symplify.store.PlayerStore store) {
    return store.player.map(p -> {
      PlayerSqs sqs = new PlayerSqs();
      sqs.originalid = p.customer_id;
      sqs.external_player_id = p.external_player_id;
      sqs.unique_id = p.unique_id;
      sqs.firstname = p.first_name;
      sqs.lastname = p.last_name;
      sqs.middlename = p.middlename;
      sqs.username = p.username;
      sqs.title = p.title;
      sqs.dateofbirth = p.birth_date.map(Transformer::truncateOrKeep);
      sqs.sex = p.sex;
      sqs.country = p.country_alpha_2_code;
      sqs.registration_country = p.current_country_name;
      sqs.current_country = p.current_country_name;
      sqs.city = p.city;
      sqs.zipcode = p.zipcode;
      sqs.address1 = p.address1;
      sqs.address2 = p.address2;
      sqs.signup_language_ID = p.language_alpha_2_code;
      sqs.signup_language = p.signup_language;
      sqs.currency_code = p.currency_code;
      sqs.is_blocked = p.is_blocked;
      sqs.is_self_excluded = p.is_self_excluded;
      sqs.account_creation_date = p.account_creation_datetime_local.map(Transformer::truncateOrKeep);
      sqs.account_verification_date = p.account_verification_datetime_local.map(Transformer::truncateOrKeep);
      sqs.emailaddress = p.email;
      sqs.phone_prefix = p.phone_prefix;
      sqs.phone = p.phone;
      sqs.mobile_phone_prefix = p.mobile_phone_prefix;
      sqs.mobile_phone = p.mobile_phone;
      sqs.mobilenumber = p.mobile;
      sqs.acquisition = p.acquisition_source_code;
      sqs.reference = p.reference;
      sqs.player_roles = p.role_name;
      sqs.player_tags = p.tag_name;
      sqs.abuser_role_tag = p.is_abuser;
      sqs.reward_indicator = p.fpp_reward_level_name;
      sqs.fpp_level = p.fpp_customer_level;
      sqs.fpp_reward_level = p.fpp_reward_level_name;
      sqs.fpp_reward_level_date_awarded = p.fpp_update_datetime_local.map(Transformer::truncateOrKeep);
      sqs.last_login_date = p.last_login_datetime_local.map(Transformer::truncateOrKeep);
      sqs.last_deposit = p.last_deposit_tenant;
      sqs.last_deposit_customer_currency = p.last_deposit_customer;
      sqs.last_deposit_date = p.last_deposit_date.map(Transformer::truncateOrKeep);
      sqs.last_withdrawal = p.last_withdrawal_tenant;
      sqs.last_withdrawal_date = p.last_withdrawal_datetime_local.map(Transformer::truncateOrKeep);
      sqs.total_balance = p.wallet_balance_total_money_tenant;
      sqs.real_money_balance = p.wallet_balance_real_money_tenant;
      sqs.bonus_money_balance = p.wallet_balance_bonus_money_tenant;
      sqs.locked_money_balance = p.wallet_balance_locked_money_tenant;
      sqs.first_deposit_date = p.first_deposit_datetime_local.map(Transformer::truncateOrKeep);
      sqs.first_withdrawal_date = p.first_withdrawal_datetime_local.map(Transformer::truncateOrKeep);
      sqs.lifetime_ngr = p.casino_lifetime_ngr_tenant;
      sqs.lifetime_ggr = p.casino_lifetime_ggr_tenant;
      sqs.lifetime_deposits = p.lifetime_deposit_tenant;
      sqs.lifetime_deposits_count = p.lifetime_deposit_count;
      sqs.lifetime_turnover = p.casino_lifetime_bet_tenant;
      sqs.lifetime_withdrawals = p.lifetime_withdrawal_tenant;
      sqs.lifetime_withdrawals_count = p.lifetime_withdrawal_count;
      sqs.sport_lifetime_average_bet = p.sport_lifetime_average_bet_tenant;
      sqs.casino_lifetime_average_bet = p.casino_lifetime_average_bet_tenant;
      sqs.casino_lifetime_real_money_ngr = p.casino_lifetime_ngr_tenant;
      sqs.lifetime_deposits_over_lifetime_bonus_cost = p.lifetime_deposit_bonus_cost_percentage_tenant;
      sqs.lifetime_sports_bet_count = p.sport_lifetime_bet_count;
      sqs.consent_marketingdirectmail = p.consent_marketing_direct_mail;
      sqs.consent_marketingoms = p.consent_marketing_oms;
      sqs.consent_marketingtelephone = p.consent_marketing_telephone;
      return sqs;
    });
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((originalid == null) ? 0 : originalid.hashCode());
    result = prime * result + ((external_player_id == null) ? 0 : external_player_id.hashCode());
    result = prime * result + ((unique_id == null) ? 0 : unique_id.hashCode());
    result = prime * result + ((firstname == null) ? 0 : firstname.hashCode());
    result = prime * result + ((lastname == null) ? 0 : lastname.hashCode());
    result = prime * result + ((middlename == null) ? 0 : middlename.hashCode());
    result = prime * result + ((username == null) ? 0 : username.hashCode());
    result = prime * result + ((title == null) ? 0 : title.hashCode());
    result = prime * result + ((dateofbirth == null) ? 0 : dateofbirth.hashCode());
    result = prime * result + ((sex == null) ? 0 : sex.hashCode());
    result = prime * result + ((country == null) ? 0 : country.hashCode());
    result = prime * result + ((registration_country == null) ? 0 : registration_country.hashCode());
    result = prime * result + ((current_country == null) ? 0 : current_country.hashCode());
    result = prime * result + ((city == null) ? 0 : city.hashCode());
    result = prime * result + ((zipcode == null) ? 0 : zipcode.hashCode());
    result = prime * result + ((address1 == null) ? 0 : address1.hashCode());
    result = prime * result + ((address2 == null) ? 0 : address2.hashCode());
    result = prime * result + ((signup_language_ID == null) ? 0 : signup_language_ID.hashCode());
    result = prime * result + ((signup_language == null) ? 0 : signup_language.hashCode());
    result = prime * result + ((currency_code == null) ? 0 : currency_code.hashCode());
    result = prime * result + ((is_blocked == null) ? 0 : is_blocked.hashCode());
    result = prime * result + ((is_self_excluded == null) ? 0 : is_self_excluded.hashCode());
    result = prime * result + ((account_creation_date == null) ? 0 : account_creation_date.hashCode());
    result = prime * result + ((account_verification_date == null) ? 0 : account_verification_date.hashCode());
    result = prime * result + ((emailaddress == null) ? 0 : emailaddress.hashCode());
    result = prime * result + ((phone_prefix == null) ? 0 : phone_prefix.hashCode());
    result = prime * result + ((phone == null) ? 0 : phone.hashCode());
    result = prime * result + ((mobile_phone_prefix == null) ? 0 : mobile_phone_prefix.hashCode());
    result = prime * result + ((mobile_phone == null) ? 0 : mobile_phone.hashCode());
    result = prime * result + ((mobilenumber == null) ? 0 : mobilenumber.hashCode());
    result = prime * result + ((acquisition == null) ? 0 : acquisition.hashCode());
    result = prime * result + ((reference == null) ? 0 : reference.hashCode());
    result = prime * result + ((player_roles == null) ? 0 : player_roles.hashCode());
    result = prime * result + ((player_tags == null) ? 0 : player_tags.hashCode());
    result = prime * result + ((abuser_role_tag == null) ? 0 : abuser_role_tag.hashCode());
    result = prime * result + ((reward_indicator == null) ? 0 : reward_indicator.hashCode());
    result = prime * result + ((fpp_level == null) ? 0 : fpp_level.hashCode());
    result = prime * result + ((fpp_reward_level == null) ? 0 : fpp_reward_level.hashCode());
    result = prime * result + ((fpp_reward_level_date_awarded == null) ? 0 : fpp_reward_level_date_awarded.hashCode());
    result = prime * result + ((last_login_date == null) ? 0 : last_login_date.hashCode());
    result = prime * result + ((last_deposit == null) ? 0 : last_deposit.hashCode());
    result = prime * result
        + ((last_deposit_customer_currency == null) ? 0 : last_deposit_customer_currency.hashCode());
    result = prime * result + ((last_deposit_date == null) ? 0 : last_deposit_date.hashCode());
    result = prime * result + ((last_withdrawal == null) ? 0 : last_withdrawal.hashCode());
    result = prime * result + ((last_withdrawal_date == null) ? 0 : last_withdrawal_date.hashCode());
    result = prime * result + ((total_balance == null) ? 0 : total_balance.hashCode());
    result = prime * result + ((real_money_balance == null) ? 0 : real_money_balance.hashCode());
    result = prime * result + ((bonus_money_balance == null) ? 0 : bonus_money_balance.hashCode());
    result = prime * result + ((locked_money_balance == null) ? 0 : locked_money_balance.hashCode());
    result = prime * result + ((first_deposit_date == null) ? 0 : first_deposit_date.hashCode());
    result = prime * result + ((first_withdrawal_date == null) ? 0 : first_withdrawal_date.hashCode());
    result = prime * result + ((lifetime_ngr == null) ? 0 : lifetime_ngr.hashCode());
    result = prime * result + ((lifetime_ggr == null) ? 0 : lifetime_ggr.hashCode());
    result = prime * result + ((lifetime_deposits == null) ? 0 : lifetime_deposits.hashCode());
    result = prime * result + ((lifetime_deposits_count == null) ? 0 : lifetime_deposits_count.hashCode());
    result = prime * result + ((lifetime_turnover == null) ? 0 : lifetime_turnover.hashCode());
    result = prime * result + ((lifetime_withdrawals == null) ? 0 : lifetime_withdrawals.hashCode());
    result = prime * result + ((lifetime_withdrawals_count == null) ? 0 : lifetime_withdrawals_count.hashCode());
    result = prime * result + ((sport_lifetime_average_bet == null) ? 0 : sport_lifetime_average_bet.hashCode());
    result = prime * result + ((casino_lifetime_average_bet == null) ? 0 : casino_lifetime_average_bet.hashCode());
    result = prime * result
        + ((casino_lifetime_real_money_ngr == null) ? 0 : casino_lifetime_real_money_ngr.hashCode());
    result = prime * result + ((lifetime_deposits_over_lifetime_bonus_cost == null) ? 0
        : lifetime_deposits_over_lifetime_bonus_cost.hashCode());
    result = prime * result + ((lifetime_sports_bet_count == null) ? 0 : lifetime_sports_bet_count.hashCode());
    result = prime * result + ((consent_marketingtelephone == null) ? 0 : consent_marketingtelephone.hashCode());
    result = prime * result + ((consent_marketingdirectmail == null) ? 0 : consent_marketingdirectmail.hashCode());
    result = prime * result + ((consent_marketingoms == null) ? 0 : consent_marketingoms.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PlayerSqs other = (PlayerSqs) obj;
    if (originalid == null) {
      if (other.originalid != null)
        return false;
    } else if (!originalid.equals(other.originalid))
      return false;
    if (external_player_id == null) {
      if (other.external_player_id != null)
        return false;
    } else if (!external_player_id.equals(other.external_player_id))
      return false;
    if (unique_id == null) {
      if (other.unique_id != null)
        return false;
    } else if (!unique_id.equals(other.unique_id))
      return false;
    if (firstname == null) {
      if (other.firstname != null)
        return false;
    } else if (!firstname.equals(other.firstname))
      return false;
    if (lastname == null) {
      if (other.lastname != null)
        return false;
    } else if (!lastname.equals(other.lastname))
      return false;
    if (middlename == null) {
      if (other.middlename != null)
        return false;
    } else if (!middlename.equals(other.middlename))
      return false;
    if (username == null) {
      if (other.username != null)
        return false;
    } else if (!username.equals(other.username))
      return false;
    if (title == null) {
      if (other.title != null)
        return false;
    } else if (!title.equals(other.title))
      return false;
    if (dateofbirth == null) {
      if (other.dateofbirth != null)
        return false;
    } else if (!dateofbirth.equals(other.dateofbirth))
      return false;
    if (sex == null) {
      if (other.sex != null)
        return false;
    } else if (!sex.equals(other.sex))
      return false;
    if (country == null) {
      if (other.country != null)
        return false;
    } else if (!country.equals(other.country))
      return false;
    if (registration_country == null) {
      if (other.registration_country != null)
        return false;
    } else if (!registration_country.equals(other.registration_country))
      return false;
    if (current_country == null) {
      if (other.current_country != null)
        return false;
    } else if (!current_country.equals(other.current_country))
      return false;
    if (city == null) {
      if (other.city != null)
        return false;
    } else if (!city.equals(other.city))
      return false;
    if (zipcode == null) {
      if (other.zipcode != null)
        return false;
    } else if (!zipcode.equals(other.zipcode))
      return false;
    if (address1 == null) {
      if (other.address1 != null)
        return false;
    } else if (!address1.equals(other.address1))
      return false;
    if (address2 == null) {
      if (other.address2 != null)
        return false;
    } else if (!address2.equals(other.address2))
      return false;
    if (signup_language_ID == null) {
      if (other.signup_language_ID != null)
        return false;
    } else if (!signup_language_ID.equals(other.signup_language_ID))
      return false;
    if (signup_language == null) {
      if (other.signup_language != null)
        return false;
    } else if (!signup_language.equals(other.signup_language))
      return false;
    if (currency_code == null) {
      if (other.currency_code != null)
        return false;
    } else if (!currency_code.equals(other.currency_code))
      return false;
    if (is_blocked == null) {
      if (other.is_blocked != null)
        return false;
    } else if (!is_blocked.equals(other.is_blocked))
      return false;
    if (is_self_excluded == null) {
      if (other.is_self_excluded != null)
        return false;
    } else if (!is_self_excluded.equals(other.is_self_excluded))
      return false;
    if (account_creation_date == null) {
      if (other.account_creation_date != null)
        return false;
    } else if (!account_creation_date.equals(other.account_creation_date))
      return false;
    if (account_verification_date == null) {
      if (other.account_verification_date != null)
        return false;
    } else if (!account_verification_date.equals(other.account_verification_date))
      return false;
    if (emailaddress == null) {
      if (other.emailaddress != null)
        return false;
    } else if (!emailaddress.equals(other.emailaddress))
      return false;
    if (phone_prefix == null) {
      if (other.phone_prefix != null)
        return false;
    } else if (!phone_prefix.equals(other.phone_prefix))
      return false;
    if (phone == null) {
      if (other.phone != null)
        return false;
    } else if (!phone.equals(other.phone))
      return false;
    if (mobile_phone_prefix == null) {
      if (other.mobile_phone_prefix != null)
        return false;
    } else if (!mobile_phone_prefix.equals(other.mobile_phone_prefix))
      return false;
    if (mobile_phone == null) {
      if (other.mobile_phone != null)
        return false;
    } else if (!mobile_phone.equals(other.mobile_phone))
      return false;
    if (mobilenumber == null) {
      if (other.mobilenumber != null)
        return false;
    } else if (!mobilenumber.equals(other.mobilenumber))
      return false;
    if (acquisition == null) {
      if (other.acquisition != null)
        return false;
    } else if (!acquisition.equals(other.acquisition))
      return false;
    if (reference == null) {
      if (other.reference != null)
        return false;
    } else if (!reference.equals(other.reference))
      return false;
    if (player_roles == null) {
      if (other.player_roles != null)
        return false;
    } else if (!player_roles.equals(other.player_roles))
      return false;
    if (player_tags == null) {
      if (other.player_tags != null)
        return false;
    } else if (!player_tags.equals(other.player_tags))
      return false;
    if (abuser_role_tag == null) {
      if (other.abuser_role_tag != null)
        return false;
    } else if (!abuser_role_tag.equals(other.abuser_role_tag))
      return false;
    if (reward_indicator == null) {
      if (other.reward_indicator != null)
        return false;
    } else if (!reward_indicator.equals(other.reward_indicator))
      return false;
    if (fpp_level == null) {
      if (other.fpp_level != null)
        return false;
    } else if (!fpp_level.equals(other.fpp_level))
      return false;
    if (fpp_reward_level == null) {
      if (other.fpp_reward_level != null)
        return false;
    } else if (!fpp_reward_level.equals(other.fpp_reward_level))
      return false;
    if (fpp_reward_level_date_awarded == null) {
      if (other.fpp_reward_level_date_awarded != null)
        return false;
    } else if (!fpp_reward_level_date_awarded.equals(other.fpp_reward_level_date_awarded))
      return false;
    if (last_login_date == null) {
      if (other.last_login_date != null)
        return false;
    } else if (!last_login_date.equals(other.last_login_date))
      return false;
    if (last_deposit == null) {
      if (other.last_deposit != null)
        return false;
    } else if (!last_deposit.equals(other.last_deposit))
      return false;
    if (last_deposit_customer_currency == null) {
      if (other.last_deposit_customer_currency != null)
        return false;
    } else if (!last_deposit_customer_currency.equals(other.last_deposit_customer_currency))
      return false;
    if (last_deposit_date == null) {
      if (other.last_deposit_date != null)
        return false;
    } else if (!last_deposit_date.equals(other.last_deposit_date))
      return false;
    if (last_withdrawal == null) {
      if (other.last_withdrawal != null)
        return false;
    } else if (!last_withdrawal.equals(other.last_withdrawal))
      return false;
    if (last_withdrawal_date == null) {
      if (other.last_withdrawal_date != null)
        return false;
    } else if (!last_withdrawal_date.equals(other.last_withdrawal_date))
      return false;
    if (total_balance == null) {
      if (other.total_balance != null)
        return false;
    } else if (!total_balance.equals(other.total_balance))
      return false;
    if (real_money_balance == null) {
      if (other.real_money_balance != null)
        return false;
    } else if (!real_money_balance.equals(other.real_money_balance))
      return false;
    if (bonus_money_balance == null) {
      if (other.bonus_money_balance != null)
        return false;
    } else if (!bonus_money_balance.equals(other.bonus_money_balance))
      return false;
    if (locked_money_balance == null) {
      if (other.locked_money_balance != null)
        return false;
    } else if (!locked_money_balance.equals(other.locked_money_balance))
      return false;
    if (first_deposit_date == null) {
      if (other.first_deposit_date != null)
        return false;
    } else if (!first_deposit_date.equals(other.first_deposit_date))
      return false;
    if (first_withdrawal_date == null) {
      if (other.first_withdrawal_date != null)
        return false;
    } else if (!first_withdrawal_date.equals(other.first_withdrawal_date))
      return false;
    if (lifetime_ngr == null) {
      if (other.lifetime_ngr != null)
        return false;
    } else if (!lifetime_ngr.equals(other.lifetime_ngr))
      return false;
    if (lifetime_ggr == null) {
      if (other.lifetime_ggr != null)
        return false;
    } else if (!lifetime_ggr.equals(other.lifetime_ggr))
      return false;
    if (lifetime_deposits == null) {
      if (other.lifetime_deposits != null)
        return false;
    } else if (!lifetime_deposits.equals(other.lifetime_deposits))
      return false;
    if (lifetime_deposits_count == null) {
      if (other.lifetime_deposits_count != null)
        return false;
    } else if (!lifetime_deposits_count.equals(other.lifetime_deposits_count))
      return false;
    if (lifetime_turnover == null) {
      if (other.lifetime_turnover != null)
        return false;
    } else if (!lifetime_turnover.equals(other.lifetime_turnover))
      return false;
    if (lifetime_withdrawals == null) {
      if (other.lifetime_withdrawals != null)
        return false;
    } else if (!lifetime_withdrawals.equals(other.lifetime_withdrawals))
      return false;
    if (lifetime_withdrawals_count == null) {
      if (other.lifetime_withdrawals_count != null)
        return false;
    } else if (!lifetime_withdrawals_count.equals(other.lifetime_withdrawals_count))
      return false;
    if (sport_lifetime_average_bet == null) {
      if (other.sport_lifetime_average_bet != null)
        return false;
    } else if (!sport_lifetime_average_bet.equals(other.sport_lifetime_average_bet))
      return false;
    if (casino_lifetime_average_bet == null) {
      if (other.casino_lifetime_average_bet != null)
        return false;
    } else if (!casino_lifetime_average_bet.equals(other.casino_lifetime_average_bet))
      return false;
    if (casino_lifetime_real_money_ngr == null) {
      if (other.casino_lifetime_real_money_ngr != null)
        return false;
    } else if (!casino_lifetime_real_money_ngr.equals(other.casino_lifetime_real_money_ngr))
      return false;
    if (lifetime_deposits_over_lifetime_bonus_cost == null) {
      if (other.lifetime_deposits_over_lifetime_bonus_cost != null)
        return false;
    } else if (!lifetime_deposits_over_lifetime_bonus_cost.equals(other.lifetime_deposits_over_lifetime_bonus_cost))
      return false;
    if (lifetime_sports_bet_count == null) {
      if (other.lifetime_sports_bet_count != null)
        return false;
    } else if (!lifetime_sports_bet_count.equals(other.lifetime_sports_bet_count))
      return false;
    if (consent_marketingtelephone == null) {
      if (other.consent_marketingtelephone != null)
        return false;
    } else if (!consent_marketingtelephone.equals(other.consent_marketingtelephone))
      return false;
    if (consent_marketingdirectmail == null) {
      if (other.consent_marketingdirectmail != null)
        return false;
    } else if (!consent_marketingdirectmail.equals(other.consent_marketingdirectmail))
      return false;
    if (consent_marketingoms == null) {
      if (other.consent_marketingoms != null)
        return false;
    } else if (!consent_marketingoms.equals(other.consent_marketingoms))
      return false;

    return true;
  }

}
