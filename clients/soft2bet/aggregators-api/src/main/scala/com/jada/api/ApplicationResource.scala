package com.jada.api

import com.jada.configuration.ApplicationConfiguration
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import org.apache.kafka.streams.KafkaStreams

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Observes
import javax.inject.Inject
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import org.jboss.logging.Logger
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.TimeUnit
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder
import java.time.LocalDateTime
import java.time.ZoneOffset
import org.jboss.resteasy.reactive.RestPath
import com.jada.processor.PlayerPartitioner
import com.jada.processor.CheckerTopology
import com.jada.processor.ApiTopology
import com.jada.processor.RepartitionerTopology
import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import com.jada.processor.Topologies
import org.apache.kafka.streams.state.QueryableStoreTypes
import com.fasterxml.jackson.databind.JsonNode
import scala.collection.mutable.ArrayDeque
import java.net.URI
import scala.util.Try
import java.time.Clock
import io.quarkus.scheduler.Scheduled
import com.jada.processor.CheckerOutside
import io.quarkus.scheduler.ScheduledExecution
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.common.serialization.StringSerializer
import java.util.ArrayList
import java.time.temporal.ChronoUnit  

@Path("/q")
@ApplicationScoped
class ApplicationResource(
    @Inject config: ApplicationConfiguration,
    @Inject mapper: ObjectMapper,
    @Inject producer: KafkaProducer[String, String],
    @Inject clock: Clock,
    @Inject @ApiTopology val apiStreams: Option[KafkaStreams],
    @Inject @CheckerTopology val checkerStreams: Option[KafkaStreams],
    @Inject @RepartitionerTopology val repartionerStreams: Option[KafkaStreams]
) {

  val streams = Map(
    ("api", apiStreams),
    ("repartioner", repartionerStreams),
    ("checker", checkerStreams)
  )

  private val logger =
    Logger.getLogger(classOf[ApplicationResource])

  val checkerKeySerializer = new StringSerializer()
  def onStart(@Observes ev: StartupEvent): Unit = {
    logger.info(s"init method ${config} ${ev}")
    streams.values.flatten.foreach(_.start())
  }

  def onStop(@Observes ev: ShutdownEvent): Unit = {
    logger.info(s"The application is stopping...${ev}")
    streams.values.flatten.foreach(_.close())
  }

  @DELETE
  @Path("/query/{queryId}")
  def deleteQuery(@RestPath("queryId") queryId: String) = {
    deleteKey(queryId)
    ResponseBuilder.accepted().build()
  }

  def deleteKey(queryId: String) = {
    val data = new ProducerRecord[String, String](
      config.topicChecker,
      queryId,
      null
    )
    producer.send(data).get(3, TimeUnit.SECONDS)
  }

  @GET
  @Path("/sports")
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def sports(
  ): Seq[JsonNode] = {
    apiStreams
      .map(s =>
        Some(
          s.store(
            fromNameAndType(
              Topologies.SportStore,
              QueryableStoreTypes.keyValueStore[String, JsonNode]()
            ).enableStaleStores()
          )
        )
      )
      .flatten
      .map(store => {
        logger.infov(
          "store sports:",
          Array(kv("nb", store.approximateNumEntries())): _*
        )
        val sports = store.all()
        val l = new ArrayDeque[JsonNode]
        while (sports.hasNext()) {
          l += sports.next().value
        }
        sports.close()
        l.toSeq.sortWith {
          case (a, b) => {
            a.get(Topologies.SportDescription).asText("") < b
              .get(Topologies.SportDescription)
              .asText("")
          }
        }
      })
      .getOrElse(Seq.empty[JsonNode])
  }

  def buildAndValidate(query: AggregatorRequest): AggregatorRequest = {
    val now = LocalDateTime.now(clock).atOffset(ZoneOffset.UTC)
    val withFrom =
      query.copy(contactId = query.contactId, from = Some(now))
    val nowWithTtl = withFrom
      .withTtl()
      .getOrElse(
        throw new BadRequestException(s"${withFrom.ttlTimeUnit} is not handled")
      )

    Try(URI.create(withFrom.acceptUri).toURL()).getOrElse(
      throw new BadRequestException(s"${withFrom.acceptUri} is not handled")
    )

    val addFromToQuery = withFrom.copy(
      ruleParameters = withFrom.ruleParameters
        .map(r =>
          r.copy(
            from = r.from.orElse(Some(now)),
            to = r.to.orElse(Some(nowWithTtl))
          )
        )
    )
    val inverseFromTo = addFromToQuery.ruleParameters.find(rule =>
      rule.from.get.isAfter(rule.to.get)
    )
    if (inverseFromTo.isDefined) {
      throw new BadRequestException(
        s"from: ${inverseFromTo.get.from} to: ${inverseFromTo.get.from} not valid"
      )
    }
    val moreThan30DaysFromTo = addFromToQuery.ruleParameters.find(rule =>
      rule.from.get.until(rule.to.get, ChronoUnit.DAYS) > 30
    )
    if (moreThan30DaysFromTo.isDefined) {
      throw new BadRequestException(
        s"from: ${moreThan30DaysFromTo.get.from} to: ${moreThan30DaysFromTo.get.from} not valid"
      )
    }
    addFromToQuery
  }
  @POST
  @Path("/query")
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  @Consumes(Array[String](MediaType.APPLICATION_JSON))
  def query(query: AggregatorRequest) = {
    logger.info(s"query: ${query}")
    val builtQuery = buildAndValidate(query)
    val key =
      s"${query.contactId}${PlayerPartitioner.Separator}${java.util.UUID.randomUUID.toString}"

    val rules = mapper.writeValueAsString(builtQuery)
    logger.infov("query with rules", kv("request", rules))
    val data = new ProducerRecord[String, String](
      config.topicChecker,
      key,
      rules
    )
    producer.send(data).get(3, TimeUnit.SECONDS)
    ResponseBuilder
      .create(
        javax.ws.rs.core.Response.Status.ACCEPTED,
        s"""{"key":"${key}"}""".stripMargin
      )
      .build()
  }

  @POST
  @Path("/test/{contactId}/{journeyId}")
  def test(@RestPath contactId: String, @RestPath journeyId: String) = {
    logger.infov(
      "success on ",
      Array(kv("contactId", contactId), kv("journeyId", journeyId)): _*
    )
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

  @POST
  @Path("/check/rule")
  def checkRule(
      @QueryParam("token") token: String,
      @QueryParam("key") key: String
  ): Unit = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      checkerStreams
        .map(s =>
          if (s.state() == KafkaStreams.State.RUNNING) {
            Option(
              (
                s.store(
                  fromNameAndType(
                    Topologies.AggregatorCheckerStore,
                    QueryableStoreTypes.keyValueStore[
                      String,
                      JsonNode
                    ]()
                  ).enableStaleStores()
                ),
                s.store(
                  fromNameAndType(
                    Topologies.AggregatorWalletStore,
                    QueryableStoreTypes.keyValueStore[
                      String,
                      JsonNode
                    ]()
                  ).enableStaleStores()
                )
              )
            )
          } else {
            None
          }
        )
        .flatten
        .map {
          case (
                checkerStore: ReadOnlyKeyValueStore[
                  String,
                  JsonNode
                ],
                depositStore: ReadOnlyKeyValueStore[
                  String,
                  JsonNode
                ]
              ) => {
            // TODO Unit and threadpool
            val checker =
              new CheckerOutside(
                clock,
                mapper,
                checkerStore,
                depositStore,
                deleteKey
              )
            checker.checkRule(key)
          }
        }
    }
  }

  @GET
  @Path("/checkers")
  @Produces(Array[String](MediaType.APPLICATION_JSON))
  def check(
      @QueryParam("token") token: String,
      @QueryParam("playerId") playerId: String
  ) = {
    val list = new ArrayList[String]()
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      checkerStreams.foreach { s =>
        {
          val d = s.store(
            fromNameAndType(
              Topologies.AggregatorCheckerStore,
              QueryableStoreTypes.keyValueStore[
                String,
                JsonNode
              ]()
            ).enableStaleStores()
          )
          val it = d.prefixScan(playerId, checkerKeySerializer)
          while (it.hasNext()) {
            val l = it.next()
            list.add(l.value.toPrettyString())
          }
        }
      }
    }
    list
  }

  @POST
  @Path("/check")
  def check(
      @QueryParam("token") token: String,
      @QueryParam("time") time: Long
  ): Unit = {
    if (token == "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4") {
      checkerStreams
        .map(s =>
          if (s.state() == KafkaStreams.State.RUNNING) {
            Option(
              (
                s.store(
                  fromNameAndType(
                    Topologies.AggregatorCheckerStore,
                    QueryableStoreTypes.keyValueStore[
                      String,
                      JsonNode
                    ]()
                  ).enableStaleStores()
                ),
                s.store(
                  fromNameAndType(
                    Topologies.AggregatorWalletStore,
                    QueryableStoreTypes.keyValueStore[
                      String,
                      JsonNode
                    ]()
                  ).enableStaleStores()
                )
              )
            )
          } else {
            None
          }
        )
        .flatten
        .map {
          case (
                checkerStore: ReadOnlyKeyValueStore[
                  String,
                  JsonNode
                ],
                depositStore: ReadOnlyKeyValueStore[
                  String,
                  JsonNode
                ]
              ) => {
            // TODO Unit and threadpool
            val checker =
              new CheckerOutside(
                clock,
                mapper,
                checkerStore,
                depositStore,
                deleteKey
              )
            checker.checkAll(time)
          }
        }
    }
  }

  @Scheduled(cron = "{cron.check.all}")
  // TODO should be a punctuator?
  def checkAll(execution: ScheduledExecution): Unit = {
    check(
      "eb20ec2f-81b2-4ad0-82bd-5cfa796d43b4",
      execution.getFireTime().getEpochSecond()
    )
  }

}
