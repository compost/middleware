package com.soft2bet.processor.players

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.soft2bet.Common
import com.soft2bet.model._
import com.soft2bet.serdes.CirceSerdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.slf4j.LoggerFactory
import org.apache.kafka.streams.scala._

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import javax.enterprise.context.ApplicationScoped
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import javax.inject.Inject
import javax.enterprise.inject.Produces
import software.amazon.awssdk.services.sqs.SqsClientBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import com.soft2bet.model.PlayerSegmentation
import com.soft2bet.model.SportPush
import com.soft2bet.model.FunidPlayerStore
import com.soft2bet.model.PlayerKPI

@ApplicationScoped
class Topologies @Inject() (
    config: com.jada.configuration.ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient
) {

  val stringSerde: Serde[String] = Serdes.String
  var ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient =
    software.amazon.awssdk.services.sqs.SqsClient
      .builder()
      .region(software.amazon.awssdk.regions.Region.EU_NORTH_1)
      .build()

  import io.circe.generic.auto._

  @Produces @com.jada.FunidTopology
  def buildFunidTopology(): Option[Topology] = {

    if (config.funidTopologyEnabled) {
      val playerStoreName = "funid-processor-store"
      val builder = new StreamsBuilder

      val playersStore = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(playerStoreName),
        stringSerde,
        CirceSerdes.serde[FunidPlayerStore]
      )
      builder.addStateStore(playersStore)
      builder
        .stream[String, Array[Byte]](
          List(
            Common.playersRepartitionedTopic,
            Common.walletRepartitionedTopic,
            Common.verificationRepartitionedTopic
          ).toSet
        )(Consumed `with` (stringSerde, new Serdes.ByteArraySerde()))
        .filter((_, v) => v != null)
        .transform(
          () =>
            new FunidTransformer(
              config,
              sqs,
              ueNorthSQS,
              playerStoreName
            ),
          playerStoreName
        )
      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.SportPushTopology
  def buildSportPushTopology(): Option[Topology] = {

    if (config.sportPushTopologyEnabled) {
      val builder = new StreamsBuilder
      builder
        .stream[String, SportPush](
          List(Common.sportPushTopic).toSet
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[SportPush]))
        .filter((_, v) => v != null)
        .transform(() =>
          new SportPushTransformer(
            config,
            sqs,
            ueNorthSQS
          )
        )
      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.PlayerSegmentationTopology
  def buildPlayerSegmentationTopology(): Option[Topology] = {

    if (config.playerSegmentationTopologyEnabled) {

      val builder = new StreamsBuilder
      val playerPunctuatorStore = "player-segmentation-punctuator"
      val storePlayerPunctuator = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(playerPunctuatorStore),
        stringSerde,
        CirceSerdes.serde[PlayerSegmentation]
      )

      builder.addStateStore(storePlayerPunctuator)
      builder
        .stream[String, PlayerSegmentation](
          Common.playerSegmentationTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[PlayerSegmentation]))
        .filter((_, v) => v.player_id.isDefined && v.brand_id.isDefined)
        .selectKey((_, v) =>
          s"${Sender.prefix(v.brand_id.get)}-${v.player_id.get}"
        )
        .to(Common.playerSegmentationRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[PlayerSegmentation])
        )

      builder
        .stream[String, PlayerSegmentation](
          List(Common.playerSegmentationRepartitionedTopic).toSet
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[PlayerSegmentation]))
        .filter((_, v) => v != null)
        .transform(
          () =>
            new PlayerSegmentationPunctuatorTransformer(
              config,
              sqs,
              ueNorthSQS,
              playerPunctuatorStore
            ),
          playerPunctuatorStore
        )

      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.LifetimeDepositCountTopology
  def buildLifetimeDepositCountTopology(): Option[Topology] = {

    if (config.lifetimeDepositCountTopologyEnabled) {

      val walletStore = "wallet"
      val wageringStore = "wagering"
      val loginStore = "login"
      val storeWallet = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(walletStore),
        stringSerde,
        CirceSerdes.serde[Wallet]
      )
      val storeWagering = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(wageringStore),
        stringSerde,
        CirceSerdes.serde[Wagering]
      )

      val storeLogin = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(loginStore),
        stringSerde,
        CirceSerdes.serde[Login]
      )

      val builder = new StreamsBuilder
      builder.addStateStore(storeWallet)
      builder.addStateStore(storeWagering)
      builder.addStateStore(storeLogin)

       builder
        .stream[String, Wallet](
          Common.walletTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Wallet]))
        .filter((_, v) => v.player_id.isDefined && v.brand_id.isDefined)
        .selectKey((_, v) =>
          s"${Sender.prefix(v.brand_id.get)}-${v.player_id.get}"
        )
        .transform(
          () =>
            new LifetimeDepositCountTransformer(
              config,
              sqs,
              ueNorthSQS,
              walletStore,
            ),
          walletStore,
        )

      builder
        .stream[String, Wagering](
          Common.wageringTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Wagering]))
        .filter((_, v) => v.player_id.isDefined && v.brand_id.isDefined)
        .selectKey((_, v) =>
          s"${Sender.prefix(v.brand_id.get)}-${v.player_id.get}"
        )
        .transform(
          () =>
            new BalanceTransformer(
              config,
              sqs,
              ueNorthSQS,
              wageringStore
            ),
          wageringStore
        )

      builder
        .stream[String, Login](
          Common.loginTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Login]))
        .filter((_, v) => v.player_id.isDefined && v.brand_id.isDefined)
        .selectKey((_, v) =>
          s"${Sender.prefix(v.brand_id.get)}-${v.player_id.get}"
        )
        .transform(
          () =>
            new LoginTransformer(
              config,
              sqs,
              ueNorthSQS,
              loginStore
            ),
          loginStore
        )

      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.PlayerKPITopology
  def buildPlayerKPITopology(): Option[Topology] = {

    if (config.playerKPITopologyEnabled) {

      val playerKPIStore = "player-kpi"

      val storePlayerKPI = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(playerKPIStore),
        stringSerde,
        CirceSerdes.serde[PlayerKPI]
      )

      val builder = new StreamsBuilder
      builder.addStateStore(storePlayerKPI)

      builder
        .stream[String, PlayerKPI](
          config.topicKPI
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[PlayerKPI]))
        .filter((_, v) => v.player_id.isDefined && v.brand_id.isDefined)
        .selectKey((_, v) =>
          s"${Sender.prefix(v.brand_id.get)}-${v.player_id.get}"
        )
        .transform(
          () =>
            new PlayerKPITransformer(
              config,
              sqs,
              ueNorthSQS,
              playerKPIStore
            ),
          playerKPIStore
        )
      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.LoginTopology
  def buildLoginTopology(): Option[Topology] = {

    if (config.loginTopologyEnabled) {

      val builder = new StreamsBuilder

      builder
        .stream[String, Login](
          Common.loginRepartitionedTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Login]))
        .transform(() =>
          new LoginProcessor(
            config,
            sqs,
            ueNorthSQS
          )
        )
      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.FixBlockedTopology
  def buildFixBlockedTopology(): Option[Topology] = {

    if (config.fixBlockedTopologyEnabled) {

      val playerStoreName = "fix-blocked-store"
      val builder = new StreamsBuilder

      val playersStore = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(playerStoreName),
        stringSerde,
        CirceSerdes.serde[PlayerStore]
      ).withLoggingDisabled()

      builder.addStateStore(playersStore)

      builder
        .stream[String, PlayerStore](
          "soft2bet-players-app-v8-players-processor-store-changelog"
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[PlayerStore]))
        .filter((_, v) =>
          v.player_id.isDefined && v.brand_id.isDefined && v.is_blocked.isDefined && !v.is_blocked
            .map(_.toLowerCase())
            .contains("false")
        )
        .selectKey((_, v) =>
          s"${Sender.prefix(v.brand_id.get)}-${v.brand_id.get}-${v.player_id.get}"
        )
        .transform(() =>
          new FixBlockedTransformer(
            config,
            sqs,
            ueNorthSQS,
            playerStoreName
          ), playerStoreName
        )
      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.FirstDepositLossTopology
  def buildFirstDepositLossTopology(): Option[Topology] = {

    if (config.firstDepositLossTopologyEnabled) {

      val sentStoreName = "fdl-sent"

      val sentSttore = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(sentStoreName),
        stringSerde,
        CirceSerdes.serde[Boolean]
      )

      val builder = new StreamsBuilder
      builder.addStateStore(sentSttore)

      builder
        .stream[String, Wagering](Common.wageringRepartitionedTopic)(
          Consumed
            .`with`(stringSerde, CirceSerdes.serde[Wagering])
        )
        .transform(
          () =>
            new WageringTransformer(
              config,
              sqs,
              ueNorthSQS,
              sentStoreName
            ),
          sentStoreName
        )
      Some(builder.build())
    } else {
      None
    }

  }

  @Produces @com.jada.RepartitionerTopology
  def buildRepartitionerTopology(): Option[Topology] = {
    if (config.repartitionerTopologyEnabled) {
      val builder = new StreamsBuilder

      builder
        .stream[String, Player](
          Common.playersTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Player]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.playersRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[Player])
        )

      builder
        .stream[String, Verification](
          Common.verificationTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Verification]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.verificationRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[Verification])
        )

      builder
        .stream[String, AccountFrozen](
          Common.accountfrozenTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[AccountFrozen]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.accountfrozenRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[AccountFrozen])
        )

      builder
        .stream[String, Achievement](
          Common.achievementTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Achievement]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.achievementRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[Achievement])
        )

      builder
        .stream[String, Wallet](
          Common.walletTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Wallet]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.walletRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[Wallet])
        )

      builder
        .stream[String, Wagering](
          Common.wageringTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Wagering]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.wageringRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[Wagering])
        )

      builder
        .stream[String, Login](
          Common.loginTopic
        )(Consumed.`with`(stringSerde, CirceSerdes.serde[Login]))
        .filter((_, v) => v.player_id.isDefined)
        .selectKey((_, v) => v.player_id.get)
        .to(Common.loginRepartitionedTopic)(
          Produced.`with`(stringSerde, CirceSerdes.serde[Login])
        )

      Some(builder.build())
    } else {
      None
    }
  }
  @Produces @com.jada.DefaultTopology
  def buildTopology(): Option[Topology] = {
    if (config.defaultTopologyEnabled) {
      val playerStoreName = "players-processor-store"
      val builder = new StreamsBuilder

      val playersStore = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(playerStoreName),
        stringSerde,
        CirceSerdes.serde[PlayerStore]
      )

      builder.addStateStore(playersStore)

      builder
        .stream[String, Array[Byte]](
          List(
            Common.playersRepartitionedTopic,
            Common.achievementRepartitionedTopic,
            Common.accountfrozenRepartitionedTopic,
            Common.walletRepartitionedTopic,
            Common.loginRepartitionedTopic,
            Common.verificationRepartitionedTopic
          ).toSet
        )(Consumed `with` (stringSerde, new Serdes.ByteArraySerde()))
        .filter((_, v) => v != null)
        .transform(
          () =>
            new PlayersTransformer(
              config,
              sqs,
              ueNorthSQS,
              playerStoreName
            ),
          playerStoreName
        )

      Some(builder.build())
    } else {
      None
    }
  }

  @Produces @com.jada.MissingDataTopology
  def buildMissinDataTopology(): Option[Topology] = {

    if (config.missingDataTopologyEnabled) {

      val sentStoreName = "missing-data"

      val sentSttore = Stores
        .keyValueStoreBuilder(
          Stores.persistentKeyValueStore(sentStoreName),
          stringSerde,
          CirceSerdes.serde[PlayerStore]
        )
        .withLoggingDisabled()

      val builder = new StreamsBuilder
      builder.addStateStore(sentSttore)

      builder
        .stream[String, PlayerStore](
          "soft2bet-players-app-v8-players-processor-store-changelog"
        )(
          Consumed
            .`with`(stringSerde, CirceSerdes.serde[PlayerStore])
        )
        .filter((_, v) =>
          v.player_id.isDefined && v.brand_id.isDefined && Sender.CasinoInfinity
            .contains(v.brand_id.get)
        )
        .transform(
          () =>
            new MissingDataTransformer(
              config,
              sqs,
              ueNorthSQS,
              sentStoreName
            ),
          sentStoreName
        )
      Some(builder.build())
    } else {
      None
    }

  }

}
