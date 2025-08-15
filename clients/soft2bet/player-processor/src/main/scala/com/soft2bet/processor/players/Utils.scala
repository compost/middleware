package com.soft2bet.processor.players

import java.time.{Instant, LocalDateTime, LocalTime, ZoneId}

object Utils {

  val UTC = ZoneId.of("UTC")

  def canBeExecuted(timestamp: Long): Boolean = {
    val dateTime = LocalDateTime.ofInstant(
      Instant.ofEpochMilli(timestamp),
      UTC
    )
    val onePm = LocalTime.of(13, 0, 0)
    val tenPm = LocalTime.of(22, 0, 0)
    val timeToCheck = dateTime.toLocalTime()
  
    val noImportInThisSlot = timeToCheck.isAfter(onePm) && timeToCheck.isBefore(tenPm)

    !(noImportInThisSlot)
  }

}
