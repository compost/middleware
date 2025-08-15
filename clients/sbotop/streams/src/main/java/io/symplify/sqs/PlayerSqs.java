package io.symplify.sqs;

import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class PlayerSqs {
  public Optional<String> originalId = Optional.empty();
  public Optional<String> reg_date = Optional.empty();
  public Optional<String> firstName = Optional.empty();
  public Optional<String> lastName = Optional.empty();
  public Optional<String> emailAddress = Optional.empty();
  public Optional<String> mobileNumber = Optional.empty();
  public Optional<String> language = Optional.empty();
  public Optional<String> affiliate_id = Optional.empty();
  public Optional<String> is_self_excluded = Optional.empty();
  public Optional<String> first_deposit_date = Optional.empty();
  public Optional<String> dateOfBirth = Optional.empty();
  public Optional<String> country_id = Optional.empty();
  public Optional<String> vip = Optional.empty();
  public Optional<String> test_user = Optional.empty();
  public Optional<String> currency = Optional.empty();
  public Optional<String> brand_id = Optional.empty();

  public PlayerSqs() {
  }

  public static Optional<PlayerSqs> transform(io.symplify.store.PlayerStore store) {
    return store.player.map(p -> {
      PlayerSqs sqs = new PlayerSqs();
      sqs.originalId = p.player_id;
      sqs.reg_date = p.reg_datetime.map(Transformer::truncateOrKeep);
      sqs.firstName = p.first_name;
      sqs.lastName = p.last_name;
      sqs.emailAddress = p.email;
      sqs.mobileNumber = p.phone_number;
      sqs.language = p.language;
      sqs.affiliate_id = p.affiliate_id;
      sqs.is_self_excluded = p.is_self_excluded;
      sqs.first_deposit_date = p.first_dep_datetime.map(Transformer::truncateOrKeep);
      sqs.dateOfBirth = p.dob.map(Transformer::truncateOrKeep);
      sqs.country_id = p.country_id;
      sqs.vip = p.VIP;
      sqs.test_user = p.test_user;
      sqs.currency = p.currency_id;
      sqs.brand_id = p.brand_id;
      return sqs;
    });
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((originalId == null) ? 0 : originalId.hashCode());
    result = prime * result + ((reg_date == null) ? 0 : reg_date.hashCode());
    result = prime * result + ((firstName == null) ? 0 : firstName.hashCode());
    result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
    result = prime * result + ((emailAddress == null) ? 0 : emailAddress.hashCode());
    result = prime * result + ((mobileNumber == null) ? 0 : mobileNumber.hashCode());
    result = prime * result + ((language == null) ? 0 : language.hashCode());
    result = prime * result + ((affiliate_id == null) ? 0 : affiliate_id.hashCode());
    result = prime * result + ((is_self_excluded == null) ? 0 : is_self_excluded.hashCode());
    result = prime * result + ((first_deposit_date == null) ? 0 : first_deposit_date.hashCode());
    result = prime * result + ((dateOfBirth == null) ? 0 : dateOfBirth.hashCode());
    result = prime * result + ((country_id == null) ? 0 : country_id.hashCode());
    result = prime * result + ((vip == null) ? 0 : vip.hashCode());
    result = prime * result + ((test_user == null) ? 0 : test_user.hashCode());
    result = prime * result + ((currency == null) ? 0 : currency.hashCode());
    result = prime * result + ((brand_id == null) ? 0 : brand_id.hashCode());
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
    if (originalId == null) {
      if (other.originalId != null)
        return false;
    } else if (!originalId.equals(other.originalId))
      return false;
    if (reg_date == null) {
      if (other.reg_date != null)
        return false;
    } else if (!reg_date.equals(other.reg_date))
      return false;
    if (firstName == null) {
      if (other.firstName != null)
        return false;
    } else if (!firstName.equals(other.firstName))
      return false;
    if (lastName == null) {
      if (other.lastName != null)
        return false;
    } else if (!lastName.equals(other.lastName))
      return false;
    if (emailAddress == null) {
      if (other.emailAddress != null)
        return false;
    } else if (!emailAddress.equals(other.emailAddress))
      return false;
    if (mobileNumber == null) {
      if (other.mobileNumber != null)
        return false;
    } else if (!mobileNumber.equals(other.mobileNumber))
      return false;
    if (language == null) {
      if (other.language != null)
        return false;
    } else if (!language.equals(other.language))
      return false;
    if (affiliate_id == null) {
      if (other.affiliate_id != null)
        return false;
    } else if (!affiliate_id.equals(other.affiliate_id))
      return false;
    if (is_self_excluded == null) {
      if (other.is_self_excluded != null)
        return false;
    } else if (!is_self_excluded.equals(other.is_self_excluded))
      return false;
    if (first_deposit_date == null) {
      if (other.first_deposit_date != null)
        return false;
    } else if (!first_deposit_date.equals(other.first_deposit_date))
      return false;
    if (dateOfBirth == null) {
      if (other.dateOfBirth != null)
        return false;
    } else if (!dateOfBirth.equals(other.dateOfBirth))
      return false;
    if (country_id == null) {
      if (other.country_id != null)
        return false;
    } else if (!country_id.equals(other.country_id))
      return false;
    if (vip == null) {
      if (other.vip != null)
        return false;
    } else if (!vip.equals(other.vip))
      return false;
    if (test_user == null) {
      if (other.test_user != null)
        return false;
    } else if (!test_user.equals(other.test_user))
      return false;
    if (currency == null) {
      if (other.currency != null)
        return false;
    } else if (!currency.equals(other.currency))
      return false;
    if (brand_id == null) {
      if (other.brand_id != null)
        return false;
    } else if (!brand_id.equals(other.brand_id))
      return false;
    return true;
  }

}
