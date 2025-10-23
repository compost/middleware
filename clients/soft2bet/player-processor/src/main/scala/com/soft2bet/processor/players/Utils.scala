package com.soft2bet.processor.players

import java.time.{Instant, LocalDateTime, LocalTime, ZoneId}

object Utils {

  val UTC = ZoneId.of("UTC")

  def canBeExecuted(timestamp: Long): Boolean = {
    val dateTime = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(timestamp),
      UTC
    )
    val after = LocalTime.of(4, 0, 0)
    val before = LocalTime.of(12, 0, 0)
    val timeToCheck = dateTime.toLocalTime()

    return timeToCheck.isAfter(after) && timeToCheck.isBefore(before)
  }

}
