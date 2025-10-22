package com.jada

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import scala.jdk.CollectionConverters._
import java.util.Locale

import com.ibm.icu.util.TimeZone
import com.ibm.icu.util.ULocale
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.time.LocalTime
import com.jada.models.PlayerStore

object WebPush {

  val countries = Locale
    .getISOCountries()
    .map(code => new Locale("", code))
    .map(l => {
      val timezone = TimeZone
        .getAvailableIDs(
          TimeZone.SystemTimeZoneType.CANONICAL_LOCATION,
          l.getCountry(),
          null
        )
        .asScala
        .headOption
        .map(t => ZoneId.of(t))
      List(
        l.getCountry -> timezone,
        l.getISO3Country -> timezone
      )
    })
    .flatten
    .toMap

  def parse(inputDateTime: String): ZonedDateTime = {
    val formatter = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(ZoneId.of("GMT"))
    ZonedDateTime.parse(inputDateTime, formatter)
  }
  def getMappingSelectorPlayerStore(
      p: PlayerStore,
      inputDateTime: Option[String],
      now: ZonedDateTime
  ): (Option[String], Option[PlayerStore]) = {

    val (mp, dailies, happyHour, nightOwl, earlyBird) = getMappingSelector(
      inputDateTime,
      p.wagering_total_balance,
      now,
      p.country_description,
      p.dailies,
      p.happyHour,
      p.nightOwl,
      p.earlyBird
    )
    if (mp.isDefined) {
      (
        mp,
        Some(
          p.copy(
            dailies = dailies,
            happyHour = happyHour,
            nightOwl = nightOwl,
            earlyBird = earlyBird
          )
        )
      )

    } else {
      (None, None)
    }

  }
  def getMappingSelector(
      inputDateTime: Option[String],
      totalBalance: Option[String],
      now: ZonedDateTime = ZonedDateTime.now(ZoneId.of("GMT")),
      countryCode: Option[String],
      dailies: Option[String] = None,
      happyHour: Option[String] = None,
      nightOwl: Option[String] = None,
      earlyBird: Option[String] = None
  ): (
      Option[String],
      Option[String],
      Option[String],
      Option[String],
      Option[String]
  ) = {
    val defaultValue = (None, None, None, None, None)
    totalBalance
      .map(_.toDouble)
      .filter(_ < 5)
      .map(_ => inputDateTime.map(d => parse(d)))
      .flatten
      .filter(date => {
        val nowMinus30Minutes = now.minusMinutes(30)
        date.isAfter(nowMinus30Minutes)
      })
      .map(date => {
        if (
          isNightOwl(date, countryCode) && nightOwl
            .map(v => parse(v))
            .map(v => ChronoUnit.HOURS.between(v, date) > 240)
            .getOrElse(true)
        ) {
          (
            Some("WebPush_NightOwl"),
            dailies,
            happyHour,
            inputDateTime,
            earlyBird
          )
        } else if (
          isEarlyBird(date, countryCode) && earlyBird
            .map(v => parse(v))
            .map(v => ChronoUnit.HOURS.between(v, date) > 240)
            .getOrElse(true)
        ) {
          (
            Some("WebPush_EarlyBird"),
            dailies,
            happyHour,
            nightOwl,
            inputDateTime
          )
        } else if (
          isHappyHour(date, countryCode) && happyHour
            .map(v => parse(v))
            .map(v => ChronoUnit.HOURS.between(v, date) > 24)
            .getOrElse(true)
        ) {
          (
            Some("WebPush_HappyHour"),
            dailies,
            inputDateTime,
            nightOwl,
            earlyBird
          )
        } else if (
          isDailies(date, countryCode) && dailies
            .map(v => parse(v))
            .map(v => ChronoUnit.HOURS.between(v, date) > 24)
            .getOrElse(true)
        ) {
          (
            Some("WebPush_Dailies"),
            inputDateTime,
            happyHour,
            nightOwl,
            earlyBird
          )
        } else {
          defaultValue
        }
      })
      .getOrElse(defaultValue)
  }
  def isHappyHour(
      inputDateTime: ZonedDateTime,
      countryCode: Option[String]
  ): Boolean = {
    isBetween(
      inputDateTime,
      countryCode,
      List((LocalTime.of(16, 0), LocalTime.of(18, 0)))
    )
  }

  def isDailies(
      inputDateTime: ZonedDateTime,
      countryCode: Option[String]
  ): Boolean = {
    isBetween(
      inputDateTime,
      countryCode,
      List(
        (LocalTime.of(18, 0), LocalTime.of(0, 0).minusNanos(1)),
        (LocalTime.of(0, 0), LocalTime.of(16, 0))
      )
    )
  }

  def isNightOwl(
      inputDateTime: ZonedDateTime,
      countryCode: Option[String]
  ): Boolean = {
    isBetween(
      inputDateTime,
      countryCode,
      List(
        (LocalTime.of(23, 0), LocalTime.of(0, 0).minusNanos(1)),
        (LocalTime.of(0, 0), LocalTime.of(4, 0))
      )
    )

  }

  def isEarlyBird(
      inputDateTime: ZonedDateTime,
      countryCode: Option[String]
  ): Boolean = {
    isBetween(
      inputDateTime,
      countryCode,
      List((LocalTime.of(4, 0), LocalTime.of(9, 0)))
    )
  }

  def isBetween(
      dateTime: ZonedDateTime,
      countryCode: Option[String],
      slots: List[(LocalTime, LocalTime)] = List.empty
  ): Boolean = {
    if (countryCode.isDefined) {
      val timeZone = countries.get(countryCode.get)

      if (timeZone.isDefined && timeZone.get.isDefined) {

        val targetDateTime =
          dateTime.withZoneSameInstant(timeZone.get.get).toLocalTime()

        slots
          .map {
            case (startTime: LocalTime, endTime: LocalTime) => {
              !targetDateTime.isBefore(startTime) && targetDateTime.isBefore(
                endTime
              )
            }
          }
          .foldLeft(false)(_ || _)
      } else {
        false
      }
    } else {
      false
    }
  }
}
