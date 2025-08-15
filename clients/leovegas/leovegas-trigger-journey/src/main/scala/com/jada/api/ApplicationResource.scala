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
import org.jboss.logging.Logger

@Path("/application")
@ApplicationScoped
class ApplicationResource @Inject()(
    config: ApplicationConfiguration,
    streams: KafkaStreams,
    kafkaProducer: KafkaProducer[String, String]
) {

  private final val logger =
    Logger.getLogger(classOf[ApplicationResource])
  val running = new AtomicBoolean(false)

  def onStart(@Observes ev: StartupEvent): Unit = {
    if (config.startup) {
      logger.debug(s"init method ${config}")
      start()
    } else {
      logger.debug("nothing on startup")
    }
  }

  def onStop(@Observes ev: ShutdownEvent) {
    streams.close();
  }

  def onStop(@Observes ev: Nothing): Unit = {
    logger.debug("The application is stopping...")
  }

  @GET
  @Path("/start")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def start() = {
    logger.debugv("start", kv("instance-id", System.getenv("WEBSITE_SITE_NAME")))

    if (running.compareAndSet(false, true)) {
      if (!KafkaStreams.State.RUNNING.equals(streams.state())) {
        streams.start()
      }
      send(Common.startCommand)
    } else {
      logger.debug("already running")
    }
  }

  @GET
  @Path("/status")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def status() = {
    logger.debugv("status", kv("state", streams.state()))
    if (streams.state().isRunningOrRebalancing()) {
      streams.state()
    } else {
      Response.serverError().entity(streams.state()).build()
    }
  }

  @GET
  @Path("/stop")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def stop() = {
    if (running.compareAndSet(true, false)) {
      send(Common.stopCommand)
      logger.debug("stopping")
    } else {
      logger.debug("already stopped")
    }
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
