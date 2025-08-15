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
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import com.leovegas.model.DataBet

import io.circe.Printer
@Path("/application")
@ApplicationScoped
class ApplicationResource @Inject() (
    config: ApplicationConfiguration,
    streams: KafkaStreams,
    kafkaProducer: KafkaProducer[String, String]
) {

  private final val logger =
    Logger.getLogger(classOf[ApplicationResource])
  val running = new AtomicBoolean(false)

  final val printer: Printer = Printer(
    dropNullValues = true,
    indent = ""
  )
  def onStart(@Observes ev: StartupEvent): Unit = {
    if (config.startup) {
      logger.info(s"init method ${config}")
      start()
    } else {
      logger.info("nothing on startup")
    }
  }

  def onStop(@Observes ev: ShutdownEvent) {
    streams.close();
  }

  def onStop(@Observes ev: Nothing): Unit = {
    logger.info("The application is stopping...")
  }

  @GET
  @Path("/start")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def start() = {
    logger.infov("start", kv("instance-id", System.getenv("WEBSITE_SITE_NAME")))

    if (running.compareAndSet(false, true)) {
      if (!KafkaStreams.State.RUNNING.equals(streams.state())) {
        streams.start()
      }
    } else {
      logger.info("already running")
    }
  }

  @GET
  @Path("/store")
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def players(
      @QueryParam("brandId") brandId: String,
      @QueryParam("playerId") playerId: String,
      @QueryParam("betId") betId: String,
      @QueryParam("token") token: String,
      @QueryParam("sqs") sqs: Boolean
  ): String = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      Option(streams)
        .map(s => {
          val store = s
            .store(
              fromNameAndType(
                "databet-store",
                QueryableStoreTypes.keyValueStore[String, DataBet]()
              ).enableStaleStores()
            )
          val p = Option(
            store
              .get(s"${brandId}-${playerId}-${betId}")
          )
          p.map(v => printer.print(DataBet.dataBetEncoder.apply(v)))
            .getOrElse(s"${store.approximateNumEntries()}")
        })
        .getOrElse("")
    } else {
      "ko"
    }
  }
  @GET
  @Path("/status")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def status() = {
    logger.infov("status", kv("state", streams.state()))
    if (streams.state().isRunningOrRebalancing()) {
      streams.state()
    } else {
      Response.serverError().entity(streams.state()).build()
    }
  }

}
