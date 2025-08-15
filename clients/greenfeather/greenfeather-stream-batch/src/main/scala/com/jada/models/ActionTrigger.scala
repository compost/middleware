package com.jada.models

case class ActionTrigger(
                          action: Option[String] = None,
                          action_value: Option[String] = None,
                          action_timestamp: Option[String] = None,
                          player_id: Option[String] = None,
                          brand_id: Option[String] = None
                        )
