package com.jada.models

case class Event(
    event_id: Option[String],
    event_description: Option[String],
    event_datetime: Option[String],
)
