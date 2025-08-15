package io.symplify.streams;

import java.util.Optional;
import io.symplify.sqs.PlayerSqs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class SenderTest {

  @Test
  public void testEncode() {
    var sender = new Sender();
    var player = new PlayerSqs();
    player.originalId = Optional.of("youpi");
    Assertions.assertEquals(
        "{\"type\":\"v-type\",\"contactId\":\"v-contact-id\",\"mappingSelector\":\"v-mapping-selector\",\"properties\":{\"originalId\":\"youpi\"}}",
        sender.encode("v-brand-id", "v-contact-id", "v-type", "v-mapping-selector", player));
  }

}
