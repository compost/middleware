package com.jada.models

//{"player_id":"125541","brand_id":"96","test_user":"false","player_status":"ACTIVE",
// "roles":"UNVERIFIED","player_label":"",
// "politically_exposed_person":"false","assigned_office":null}
case class PlayerExtended(
    player_id: Option[String] = None,
    brand_id: Option[String] = None,
    test_user: Option[String] = None,
    player_status: Option[String] = None,
    roles: Option[String] = None,
    player_label: Option[String] = None,
    politically_exposed_person: Option[String] = None,
    assigned_office: Option[String] = None
)

