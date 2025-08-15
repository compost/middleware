package com.jada

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions

import java.time.Instant
import java.time.Clock
import java.time.ZoneOffset

import WebPush._
class WebPushTest {

  @Test
  def getMappingSelector(): Unit = {
    Assertions.assertEquals(
      (
        None,
        None,
        None,
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          None,
          None,
          parse("2023-12-12 07:01:00"),
          Some("JPN")
        )
    )
    Assertions.assertEquals(
      (
        None,
        None,
        None,
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 07:00:00"),
          None,
          parse("2023-12-12 07:01:00"),
          Some("JPN")
        )
    )

    Assertions.assertEquals(
      (
        None,
        None,
        None,
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 07:00:00"),
          Some("5.0"),
          parse("2023-12-12 07:01:00"),
          Some("JPN")
        )
    )
    Assertions.assertEquals(
      (
        None,
        None,
        None,
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          None,
          Some("5.0"),
          parse("2023-12-12 07:01:00"),
          Some("JPN")
        )
    )

    Assertions.assertEquals(
      (
        None,
        None,
        None,
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 07:00:00"),
          Some("4.99"),
          parse("2023-12-12 07:31:00"),
          Some("JPN")
        ),
      "30 minutes"
    )
    Assertions.assertEquals(
      (
        Some("WebPush_HappyHour"),
        None,
        Some("2023-12-12 07:00:00"),
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 07:00:00"),
          Some("4.99"),
          parse("2023-12-12 07:01:00"),
          Some("JPN")
        )
    )
    Assertions.assertEquals(
      (
        None,
        None,
        None,
        None,
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 07:01:00"),
          Some("4.99"),
          parse("2023-12-12 07:01:00"),
          Some("JPN"),
          Some("2023-12-12 06:01:00"),
          Some("2023-12-12 06:01:00")
        )
    )

    Assertions.assertEquals(
      (
        Some("WebPush_NightOwl"),
        None,
        None,
        Some("2023-12-12 23:59:59"),
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 23:59:59"),
          Some("3.33"),
          parse("2023-12-12 23:59:59"),
          Some("GHA")
        )
    )

    Assertions.assertEquals(
      (
        Some("WebPush_Dailies"),
        Some("2023-12-12 23:59:59"),
        None,
        Some("2023-12-11 23:59:59"),
        None
      ),
      WebPush
        .getMappingSelector(
          Some("2023-12-12 23:59:59"),
          Some("3.33"),
          parse("2023-12-12 23:59:59"),
          Some("GHA"),
          None,
          None,
          Some("2023-12-11 23:59:59"),
          None
        )
    )
  }
}
