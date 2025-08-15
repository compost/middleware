package com.soft2bet

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions
import com.soft2bet.model.PlayerStoreSQS

class PlayerResourceTest {
  @Test
  def fixBrandName(): Unit = {
    Assertions.assertEquals(
      None,
      PlayerStoreSQS.fixBrandName(Some("123"), None)
    )

    Assertions.assertEquals(
      None,
      PlayerStoreSQS.fixBrandName(None, None)
    )
    Assertions.assertEquals(
      Some("expected"),
      PlayerStoreSQS.fixBrandName(Some("123"), Some("expected"))
    )
    Assertions.assertEquals(
      Some("expected"),
      PlayerStoreSQS.fixBrandName(None, Some("expected"))
    )
    Assertions.assertEquals(
      Some("ingobet"),
      PlayerStoreSQS.fixBrandName(Some("217"), Some("ddd"))
    )
    Assertions.assertEquals(
      Some("boomerang"),
      PlayerStoreSQS.fixBrandName(Some("217"), Some("boomerangcasino"))
    )

    Assertions.assertEquals(
      Some("mrpacho"),
      PlayerStoreSQS.fixBrandName(Some("217"), Some("mrpachocasino"))
    )

    Assertions.assertEquals(
      Some("betmen"),
      PlayerStoreSQS.fixBrandName(Some("217"), Some("betman"))
    )

    Assertions.assertEquals(
      Some("naobet"),
      PlayerStoreSQS.fixBrandName(Some("217"), Some("noabet"))
    )
  }
}
