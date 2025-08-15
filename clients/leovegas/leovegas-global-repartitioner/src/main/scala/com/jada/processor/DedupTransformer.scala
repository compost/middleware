package com.jada.processor

import com.jada.configuration.ApplicationConfiguration
import com.jada.models.PlayerExtended
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.jboss.logging.Logger

class DedupTransformer(
    config: ApplicationConfiguration,
    playerStoreName: String
) extends Transformer[
      String,
      PlayerExtended,
      KeyValue[String, PlayerExtended]
    ] {

  private final val logger =
    Logger.getLogger(classOf[DedupTransformer])
  private var processorContext: ProcessorContext = _
  private var extendedPlayerStore: KeyValueStore[String, PlayerExtended] = _

  override def init(processorContext: ProcessorContext): Unit = {
    logger.info(f"${config}")
    this.processorContext = processorContext
    this.extendedPlayerStore = processorContext.getStateStore(playerStoreName)
  }

  override def transform(
      key: String,
      inputPlayer: PlayerExtended
  ): KeyValue[String, PlayerExtended] = {
    val playerFromStore = extendedPlayerStore.get(key)

    val newPlayer = Option(playerFromStore) match {
      case Some(player) =>
        player.copy(
          originalId = inputPlayer.originalId,
          country_id = inputPlayer.country_id.orElse(player.country_id),
          brand_id = inputPlayer.brand_id.orElse(player.brand_id),
          licenseuid = inputPlayer.brand_id.orElse(player.licenseuid),
          country = inputPlayer.brand_id.orElse(player.country),
          operator = inputPlayer.brand_id.orElse(player.operator)
        )

      case None =>
        // partially register non null infos
        PlayerExtended(
          originalId = inputPlayer.originalId,
          country_id = inputPlayer.country_id,
          brand_id = inputPlayer.brand_id,
          licenseuid = inputPlayer.licenseuid,
          country = inputPlayer.country,
          operator = inputPlayer.operator
        )
    }

    if (playerFromStore == null || newPlayer != playerFromStore) {
      extendedPlayerStore.put(key, newPlayer)
      new KeyValue(key, newPlayer)
    } else null

  }
  override def close(): Unit = {}
}
