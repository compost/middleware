package com.jada.processor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.jada.api.AggregatorRequest
import com.jada.api.PlayerAction
import com.jada.api.PlayerActionStatus
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.kafka.streams.state.KeyValueIterator
import org.jboss.logging.Logger

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayDeque
import java.time.ZoneOffset
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import io.quarkiverse.loggingjson.providers.KeyValueStructuredArgument.kv
import java.time.Clock
import java.util.UUID
import com.jada.api.Deposit
import com.jada.api.Rule
import javax.ws.rs.core.MediaType
import org.apache.http.entity.StringEntity
import org.apache.kafka.common.serialization.StringSerializer
import java.io.IOException
import org.apache.http.client.ClientProtocolException

// checker outside kafka stream context
class CheckerOutside(
    clock: Clock,
    mapper: ObjectMapper,
    checkerStore: ReadOnlyKeyValueStore[String, JsonNode],
    depositStore: ReadOnlyKeyValueStore[String, JsonNode],
    onSuccessOrExpired: (String) => Unit
) extends Checker[JsonNode](
      clock,
      mapper,
      checkerStore,
      depositStore,
      onSuccessOrExpired
    ) {
  def value(v: JsonNode): JsonNode = v
}

// checker inside kafka stream context
class CheckerInside(
    clock: Clock,
    mapper: ObjectMapper,
    checkerStore: ReadOnlyKeyValueStore[String, ValueAndTimestamp[JsonNode]],
    depositStore: ReadOnlyKeyValueStore[String, ValueAndTimestamp[JsonNode]],
    onSuccessOrExpired: (String) => Unit
) extends Checker[ValueAndTimestamp[JsonNode]](
      clock,
      mapper,
      checkerStore,
      depositStore,
      onSuccessOrExpired
    ) {
  def value(v: ValueAndTimestamp[JsonNode]): JsonNode = v.value
}
abstract class Checker[V](
    val clock: Clock,
    val mapper: ObjectMapper,
    val checkerStore: ReadOnlyKeyValueStore[String, V],
    val depositStore: ReadOnlyKeyValueStore[String, V],
    val onSuccessOrExpired: (String) => Unit
) {

  val checkerKeySerializer = new StringSerializer()
  private val logger = Logger.getLogger(classOf[Checker[V]])
  implicit val localDateOrdering: Ordering[OffsetDateTime] =
    Ordering.by(_.toEpochSecond())

  def value(v: V): JsonNode

  /** Check the checkers for a player
    *
    * @param readOnlyKey
    *   the key with player_id at the first place
    * @return
    *   the list of checker id which are in success
    */
  def check(readOnlyKey: String, traceId: UUID): List[String] = {
    val key = readOnlyKey.split(PlayerPartitioner.Separator)
    if (key.size != 3) {
      throw new RuntimeException("do it to myself")
    }
    logger.debugv(
      s"check player ${traceId}",
      Array(
        kv("playerId", key(0)),
        kv("traceId", traceId)
      ): _*
    )
    val checkers = checkerStore.prefixScan(key(0), checkerKeySerializer)

    try {
      val successed = scala.collection.mutable.ListBuffer.empty[String]
      while (checkers.hasNext()) {
        val next = checkers.next()
        if (
          checkSubAndAction(
            next.key,
            mapper.treeToValue(value(next.value), classOf[AggregatorRequest]),
            "unit",
            traceId
          )
        ) {
          successed += next.key
        }
      }
      successed.toList
    } finally {
      checkers.close()
    }
  }

  private def checkSubAndAction(
      key: String,
      query: AggregatorRequest,
      kind: String,
      traceId: UUID = UUID.randomUUID
  ): Boolean = {
    if (checkSub(key, query, kind, traceId)) {
      onSuccessOrExpired(key)
      true
    } else {
      false
    }
  }

  def calculNbValidatedRules(
      query: AggregatorRequest,
      key: String,
      store: ReadOnlyKeyValueStore[String, V],
      builder: (KeyValueIterator[String, V]) => List[PlayerAction],
      ruleDeposit: List[Rule],
      kind: String,
      traceId: UUID
  ): (Int, List[Option[PlayerAction]]) = {
    val (aggregatedRules, simpleRules) =
      ruleDeposit.partition(_.times.isDefined)

    if (ruleDeposit.nonEmpty) {
      val (from, to) = fromTo(query.contactId, ruleDeposit)
      val data = store.range(
        from,
        to
      )

      try {
        val result = builder(data)
        logger.debugv(
          s"start calculating data ${kind} - ${result.size} - ${ruleDeposit.size} - ${aggregatedRules.size} - ${simpleRules.size}",
          Array(
            kv("size", result.size),
            kv("playerId", query.contactId),
            kv("key", key),
            kv("traceId", traceId),
            kv("from", from),
            kv("to", to)
          ): _*
        )
        val aggregrated = aggregatedRules
          .map(rule => {
            result
              .filter(p => rule.handle(query.contactId, p.key, p.sportId))
              .groupBy(f => f.playerId)
              .map {
                case (
                      playerId: String,
                      actions: List[PlayerAction]
                    ) => {
                  actions.foldLeft(
                    new PlayerActionStatus(playerId, 0, 0)
                  ) { case (v: PlayerActionStatus, s: PlayerAction) =>
                    v.copy(count = v.count + 1, sum = v.sum + s.amount)
                  }
                }
              }
              .find(p => rule.accept(p))
          })
          .count(_.isDefined)

        val toBeSent = simpleRules
          .map(rule => {
            result
              .filter(p => rule.handle(query.contactId, p.key, p.sportId))
              .find(p => rule.accept(p))
          })

        (aggregrated + toBeSent.count(_.isDefined), toBeSent)
      } finally {
        data.close()
      }
    } else {
      (0, List.empty)
    }
  }

  private def fromTo(contactId: String, rules: List[Rule]): (String, String) = {
    val min = rules
      .map(f => f.from)
      .min
      .map(_.withOffsetSameInstant(ZoneOffset.UTC).withNano(0))
    val max = rules
      .map(f => f.to)
      .max
      .map(_.withOffsetSameInstant(ZoneOffset.UTC).withNano(0))
    val from =
      s"${contactId}${PlayerPartitioner.Separator}${min.get.format(
          DateTimeFormatter.ISO_OFFSET_DATE_TIME
        )}${PlayerPartitioner.Separator}${PlayerPartitioner.UUIDFrom}"
    val to =
      s"${contactId}${PlayerPartitioner.Separator}${max.get.format(
          DateTimeFormatter.ISO_OFFSET_DATE_TIME
        )}${PlayerPartitioner.Separator}${PlayerPartitioner.UUIDTo}"

    (from, to)
  }

  private def checkSub(
      key: String,
      query: AggregatorRequest,
      kind: String,
      traceId: UUID
  ): Boolean = {
    logger.debugv(
      "start check",
      Array(
        kv("key", key),
        kv("playerId", query.contactId),
        kv("kind", kind),
        kv("traceId", traceId)
      ): _*
    )

    try {
      val (ruleDeposit, _) =
        query.ruleParameters.partition(_.kind == Deposit)
      val (nbValidatedDepositRules, actions) = calculNbValidatedRules(
        query,
        key,
        depositStore,
        buildPlayerActionFromDeposit,
        ruleDeposit,
        kind,
        traceId
      )
      val expectedRules = query.ruleParameters.size
      val total = nbValidatedDepositRules
      val goCall = total == expectedRules
      logger.debugv(
        "processing result",
        Array(
          kv("nbValidatedDepositRules", nbValidatedDepositRules),
          kv("expected", expectedRules),
          kv("total", total),
          kv("kind", kind),
          kv("call", goCall),
          kv("playerId", query.contactId),
          kv("key", key),
          kv("traceId", traceId)
        ): _*
      )
      // We expect to have exactly the same number of rules
      if (goCall) {
        logger.debugv(
          "call",
          Array(
            kv("nbValidatedDepositRules", nbValidatedDepositRules),
            kv("expected", expectedRules),
            kv("playerId", query.contactId),
            kv("kind", kind),
            kv("key", key),
            kv("traceId", traceId)
          ): _*
        )
        return call(
          key,
          query.contactId,
          query.acceptUri,
          actions.flatten,
          traceId
        )
      }
      logger.debugv(
        s"missing rules ${nbValidatedDepositRules} ${expectedRules}",
        Array(
          kv("nbValidatedDepositRules", nbValidatedDepositRules),
          kv("expected", expectedRules),
          kv("playerId", query.contactId),
          kv("key", key),
          kv("kind", kind),
          kv("traceId", traceId)
        ): _*
      )
      if (query.expired(clock)) {
        logger.debugv(
          "expired query",
          Array(
            kv("key", key),
            kv("playerId", query.contactId),
            kv("traceId", traceId)
          ): _*
        )
        return true
      }
    } finally {
      logger.debugv(
        "end check",
        Array(
          kv("key", key),
          kv("playerId", query.contactId),
          kv("traceId", traceId)
        ): _*
      )
    }

    false
  }

  // return true if the call is in success
  def call(
      key: String,
      playerId: String,
      acceptUri: String,
      actions: List[PlayerAction],
      traceId: UUID
  ): Boolean = {
    val client = HttpClientBuilder.create().disableAutomaticRetries().build()
    logger.debugv(
      "start post",
      Array(
        kv("key", key),
        kv("playerId", playerId),
        kv("acceptUri", acceptUri),
        kv("actions", actions),
        kv("traceId", traceId)
      ): _*
    )
    val post = new HttpPost(acceptUri)
    if (actions.nonEmpty) {
      post.addHeader("Content-Type", MediaType.APPLICATION_JSON)
      val body =
        s"""{"properties": {"LastDepositSum": "${actions.head.amount}"}}"""
      logger.infov(
        s"with body",
        Array(
          kv("key", key),
          kv("body", body),
          kv("playerId", playerId),
          kv("acceptUri", acceptUri),
          kv("traceId", traceId)
        ): _*
      )
      post.setEntity(new StringEntity(body))

    }
    try {
      import scala.util.Using

      Using(client.execute(post)) { response =>
        val code = response.getStatusLine().getStatusCode()
        logger.debugv(
          "post",
          Array(
            kv("key", key),
            kv("httpCode", code),
            kv("playerId", playerId),
            kv("acceptUri", acceptUri),
            kv("traceId", traceId)
          ): _*
        )
        if (code < 200 || code > 299) {
          logger.warnv(
            "cannot post",
            Array(
              kv("key", key),
              kv("playerId", playerId),
              kv("httpCode", response.getStatusLine().getStatusCode()),
              kv("acceptUri", acceptUri),
              kv("traceId", traceId)
            ): _*
          )
          code == 409
        } else {
          logger.infov(
            "post",
            Array(
              kv("key", key),
              kv("playerId", playerId),
              kv("httpCode", response.getStatusLine().getStatusCode()),
              kv("acceptUri", acceptUri),
              kv("traceId", traceId)
            ): _*
          )
          true
        }
      }.getOrElse(false)
    } catch {
      case e @ (_: ClientProtocolException | _: IOException) => {
        logger.warnv(
          "cannot post",
          Array(
            kv("key", key),
            kv("playerId", playerId),
            kv("acceptUri", acceptUri),
            kv("traceId", traceId),
            kv("error", e)
          ): _*
        )
        return false
      }
    } finally {
      logger.infov(
        "call finally",
        Array(
          kv("key", key),
          kv("playerId", playerId),
          kv("acceptUri", acceptUri),
          kv("traceId", traceId)
        ): _*
      )
    }
  }

  final def buildPlayerActionFromDeposit(
      kv: KeyValueIterator[String, V]
  ): List[PlayerAction] = {
    val l = new ArrayDeque[PlayerAction]
    while (kv.hasNext()) {
      val next = kv.next()
      l += PlayerAction(
        key = next.key,
        playerId = value(next.value).get(Topologies.PlayerId).asText(),
        amount = value(next.value).get(Topologies.Amount).asDouble(),
        sportId = None
      )
    }
    l.toList
  }

  final def buildPlayerActionFromWagering(
      kv: KeyValueIterator[String, V]
  ): List[PlayerAction] = {
    val l = new ArrayDeque[PlayerAction]
    while (kv.hasNext()) {
      val next = kv.next()
      val sportsbets =
        value(next.value).get(Topologies.WageringSport.Sportsbets)
      val wagering =
        value(next.value).get(Topologies.WageringSport.Wagering)

      l += PlayerAction(
        key = next.key,
        playerId = wagering.get(Topologies.PlayerId).asText(),
        amount = wagering
          .get(Topologies.WageringSport.WagerAmountCash)
          .asDouble(),
        sportId = Option(sportsbets.get(Topologies.SportId).asText())
      )
    }
    l.toList
  }

  def checkRule(key: String): Unit = {
    val traceId = UUID.randomUUID()
    logger.info(s"start checkRule at ${key} - ${traceId}")
    val next = checkerStore.get(key)
    if (next != null) {
      checkSubAndAction(
        key,
        mapper.treeToValue(value(next), classOf[AggregatorRequest]),
        "all",
        traceId
      )
    } else {

      logger.info(s"not found at ${key} - ${traceId}")
    }

  }
  def checkAll(timestamp: Long): Unit = {
    val traceId = UUID.randomUUID()
    logger.info(s"start checkAll at ${timestamp} - ${traceId}")
    val checkers = checkerStore.all()
    try {
      while (checkers.hasNext()) {
        val next = checkers.next()
        checkSubAndAction(
          next.key,
          mapper.treeToValue(value(next.value), classOf[AggregatorRequest]),
          "all",
          traceId
        )
      }
    } finally {
      checkers.close()
      logger.info(s"end checkAll at ${timestamp} - ${traceId}")
    }
  }
}
