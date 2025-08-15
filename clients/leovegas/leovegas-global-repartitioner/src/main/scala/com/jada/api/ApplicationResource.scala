package com.jada.api


import com.jada.configuration.ApplicationConfiguration
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.KafkaStreams

import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import java.util.concurrent.atomic.AtomicBoolean
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Observes
import javax.inject.Inject
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.jboss.logging.Logger
import org.apache.kafka.streams.state.QueryableStoreTypes
import io.circe.Printer

@Path("/application")
@ApplicationScoped
class ApplicationResource @Inject()(
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
    if (config.startup) {
      logger.info("init method")
      start()
    } else {
      logger.info("nothing on startup")
    }
  }

  def onStop(@Observes ev: ShutdownEvent) = {
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
    if (!KafkaStreams.State.RUNNING.equals(streams.state())) {
      streams.start()
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

  @GET
  @Path("/stop")
  @Produces(Array[String](MediaType.TEXT_PLAIN))
  def stop() = {
    if (running.compareAndSet(true, false)) {
      logger.info("stopping")
    } else {
      logger.info("already stopped")
    }
  }


}
