package com.jada.api

import com.amazonaws.services.sqs.AmazonSQS
import com.fasterxml.jackson.databind.JsonNode
import com.jada.Common
import com.jada.configuration.ApplicationConfiguration
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Observes
import javax.inject.Inject
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.ResponseBuilder
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import org.jboss.logging.Logger
import com.soft2bet.model.PlayerStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import io.circe.Printer
import com.jada.sqs.Body
import com.soft2bet.model.PlayerStoreSQS
import com.soft2bet.processor.players.Sender
import org.joda.time.DateTime
import com.soft2bet.model.PlayerSegmentation
import com.soft2bet.model.PlayerSegmentationSQS

@Path("/application")
@ApplicationScoped
class ApplicationResource @Inject() (
    config: ApplicationConfiguration,
    sqs: software.amazon.awssdk.services.sqs.SqsClient,
    @Inject @com.jada.DefaultTopology defaultStreams: Option[KafkaStreams],
    @Inject @com.jada.LifetimeDepositCountTopology lifetimeDepositCountStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.PlayerSegmentationTopology playerSegmentationStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.SportPushTopology sportPushStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.FunidTopology funidStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.RepartitionerTopology repartitionerStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.PlayerKPITopology playerKPIStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.FirstDepositLossTopology firstDepositLossStreams: Option[
      KafkaStreams
    ],
    @Inject @com.jada.MissingDataTopology missingDataStreams: Option[
      KafkaStreams
    ],
    kafkaProducer: KafkaProducer[String, String]
) {

  val streams = Map(
    ("default", defaultStreams),
    ("lifetimeDepositCount", lifetimeDepositCountStreams),
    ("playersegmentation", playerSegmentationStreams),
    ("sportpush", sportPushStreams),
    ("funid", funidStreams),
    ("repartitioner", repartitionerStreams),
    ("player-kpi", playerKPIStreams),
    ("fdl", firstDepositLossStreams),
    ("missing-data", missingDataStreams)
  )

  val ueNorthSQS: software.amazon.awssdk.services.sqs.SqsClient =
    software.amazon.awssdk.services.sqs.SqsClient
      .builder()
      .region(software.amazon.awssdk.regions.Region.EU_NORTH_1)
      .build()

  private final val logger =
    Logger.getLogger(classOf[ApplicationResource])

  private val sender = new Sender(config, sqs, ueNorthSQS, true, false)
  val running = new AtomicBoolean(false)

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  def onStart(@Observes ev: StartupEvent): Unit = {
    logger.debug(s"configuration: ${config}")
    if (config.startup) {
      logger.debug("init method")
      start()
    } else {
      logger.debug("nothing on startup")
    }
  }

  def onStop(@Observes ev: ShutdownEvent) = {
    streams.values.flatten.foreach(_.close())
  }

  @GET
  @Path("/fix")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def fix(@QueryParam("token") token: String) = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      import scala.io.Source
      val source =
        Source.fromInputStream(getClass.getResourceAsStream("/isverified.txt"))
      for (line <- source.getLines()) {
        val p = s"""{"brand_id":"86","player_id":"$line","IsVerified":"true"}"""
        val data = new ProducerRecord[String, String](
          com.soft2bet.Common.playersRepartitionedTopic,
          line,
          p
        )
        kafkaProducer.send(data).get(60, TimeUnit.SECONDS)
      }
      source.close()
      "END-t"

    }
    "END"
  }

  @GET
  @Path("/build")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def build() = {
    config.buildTime
  }

  @GET
  @Path("/start")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def start() = {
    if (running.compareAndSet(false, true)) {
      logger.debugv(
        "start",
        kv("instance-id", System.getenv("WEBSITE_SITE_NAME"))
      )
      streams.values.flatten.foreach(s => {
        if (!s.state().isRunningOrRebalancing()) {
          s.start()
        }
      })
      send(Common.startCommand)
    } else {
      logger.debug("already running")
    }
  }

  @GET
  @Path("/status")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def status() = {
    if (streams.isEmpty) {
      "OK"
    } else {
      val allRunningOrRebalancing =
        streams.values.flatten.forall(_.state().isRunningOrRebalancing())
      val status = streams
        .collect { case (key, Some(v)) => s"${key}:${v.state()}" }
        .mkString(";")
      if (allRunningOrRebalancing) {
        status
      } else {
        Response.serverError().entity(status).build()
      }
    }
  }

  @GET
  @Path("/player")
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def players(
      @QueryParam("playerId") playerId: String,
      @QueryParam("token") token: String,
      @QueryParam("sqs") sqs: Boolean
  ): String = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      defaultStreams
        .map(s => {
          val p = s
            .store(
              fromNameAndType(
                "players-processor-store",
                QueryableStoreTypes.keyValueStore[String, PlayerStore]()
              ).enableStaleStores()
            )
            .get(playerId)
          if (sqs) {
            sender.sendPlayer(None, None, p)
          }
          printer.print(PlayerStore.playerStoreEncoder.apply(p))
        })
        .getOrElse("")
    } else {
      "ko"
    }
  }

  @GET
  @Path("/segmentation")
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def segmentation(
      @QueryParam("key") playerId: String,
      @QueryParam("token") token: String
  ): String = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      playerSegmentationStreams
        .map(s => {
          val p = s
            .store(
              fromNameAndType(
                "player-segmentation-punctuator",
                QueryableStoreTypes.keyValueStore[String, PlayerSegmentation]()
              ).enableStaleStores()
            )
            .get(playerId)
          printer.print(
            PlayerSegmentationSQS.playerSegmentationSQSEncoder
              .apply(PlayerSegmentationSQS(p))
          )
        })
        .getOrElse("")
    } else {
      "ko"
    }
  }

  @GET
  @Path("/stop")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def stop(@QueryParam("token") token: String) = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      streams.values.flatten.foreach(_.close())
      "stopped"
    } else { "nope" }
  }

  def send(action: String) = {
    val data = new ProducerRecord[String, String](
      config.topicCoordinator,
      config.applicationId,
      action
    )
    kafkaProducer.send(data).get(60, TimeUnit.SECONDS)
  }

}
