package com.jada.api

import com.amazonaws.services.sqs.AmazonSQS
import com.fasterxml.jackson.databind.JsonNode
import com.jada.configuration.ApplicationConfiguration
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams

import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
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
import org.jboss.logging.Logger
import org.apache.kafka.streams.state.QueryableStoreTypes
import com.jada.models.PlayerStore
import io.circe.Printer
import com.jada.models.Country

@Path("/q")
@ApplicationScoped
class ApplicationResource @Inject() (
    config: ApplicationConfiguration,
    streams: KafkaStreams,
    kafkaProducer: KafkaProducer[String, String]
) {

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  private final val logger =
    Logger.getLogger(classOf[ApplicationResource])

  val running = new AtomicBoolean(false)
  def onStart(@Observes ev: StartupEvent): Unit = {
    start()
  }

  def onStop(@Observes ev: ShutdownEvent) = {
    streams.close();
  }

  def onStop(@Observes ev: Nothing): Unit = {
    logger.debug("The application is stopping...")
  }

  def start() = {
    if (running.compareAndSet(false, true)) {
      if (!KafkaStreams.State.RUNNING.equals(streams.state())) {
        streams.start()
      }
    } else {
      logger.debug("already running")
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
      Option(streams)
        .map(s => {
          val p = s
            .store(
              fromNameAndType(
                "player-store",
                QueryableStoreTypes.keyValueStore[String, PlayerStore]()
              ).enableStaleStores()
            )
            .get(playerId)
          printer.print(PlayerStore.playerStoreEncoder.apply(p))
        })
        .getOrElse("")
    } else {
      "ko"
    }
  }

  @GET
  @Path("/country")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def country(
      @QueryParam("id") id: String,
      @QueryParam("token") token: String,
      @QueryParam("sqs") sqs: Boolean
  ): String = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      Option(streams)
        .map(s => {
          val store = s
            .store(
              fromNameAndType(
                "country-store",
                QueryableStoreTypes.keyValueStore[String, Country]()
              ).enableStaleStores()
            )
          val p = store.get(id)
          s"${p} nb:${store.approximateNumEntries()}"
        })
        .getOrElse("")
    } else {
      "ko"
    }
  }

  @GET
  @Path("/health")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def status() = {
    if (streams.state().isRunningOrRebalancing()) {
      streams.state()
    } else {
      Response.serverError().entity(streams.state()).build()
    }
  }

  @GET
  @Path("/build")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def build(): String = {
    return "2024-12-19"
  }

}
