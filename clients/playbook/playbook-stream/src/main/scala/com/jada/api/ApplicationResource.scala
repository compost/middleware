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
    @Inject @com.jada.DefaultTopology defaultStreams: Option[KafkaStreams],
    @Inject @com.jada.BalanceTopology balanceStreams: Option[KafkaStreams]
) {

  val streams = Map(
    ("default", defaultStreams),
    ("balance", balanceStreams)
  )

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
    streams.values.flatten.foreach(_.close())
  }

  def onStop(@Observes ev: Nothing): Unit = {
    logger.debug("The application is stopping...")
  }

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
    } else {
      logger.debug("already running")
    }

  }

  @GET
  @Path("/health")
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

}
