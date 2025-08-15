import com.jada.configuration.ApplicationConfiguration
import io.quarkus.test.junit.QuarkusTest
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import io.restassured.RestAssured.given
import org.awaitility.Awaitility.await
import javax.inject.Inject
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Test
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import io.restassured.http.ContentType
import org.hamcrest.CoreMatchers.is
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import javax.enterprise.inject.Produces
import io.quarkus.arc.profile.IfBuildProfile
import java.time.Instant
import java.time.ZoneId
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.regex.Pattern
import org.junit.jupiter.api.Assertions
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.UUID

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApplicationTopologyTest(
    @Inject
    config: ApplicationConfiguration,
    @Inject
    adminClientKafka: AdminClient,
    @Inject
    producer: KafkaProducer[String, String]
) {

  @Produces
  @IfBuildProfile("test")
  def fixedClock(): Clock = {
    Clock.fixed(Instant.parse("1990-03-11T19:47:13.5858638Z"), ZoneId.of("UTC"))
  }

  var wireMockServer: WireMockServer = _
  @Test def shouldReturnsSports(): Unit = {
    produce(
      config.topicSport,
      List(
        (
          "key",
          """
    {"sport_id":"id_1", "sport_description":"soccer"}
    """
        ),
        (
          "key",
          """
        {"sport_id":"id_2", "sport_description":"volley"}
    """
        ),
        (
          "key",
          """
        {"sport_id":"id_1", "sport_description":"soccer"}
    """
        )
      )
    )

    await()
      .atMost(120, TimeUnit.SECONDS)
      .pollDelay(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        given()
          .contentType(ContentType.JSON)
          .when()
          .get("/application/sports")
          .`then`
          .assertThat
          .statusCode(200)
          .body(
            is(
              """[{"sport_id":"id_1","sport_description":"soccer"},{"sport_id":"id_2","sport_description":"volley"}]"""
            )
          )
      })
  }

  @Test def shouldGetABadRequest(): Unit = {
    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        given()
          .contentType(ContentType.JSON)
          .body(
            s"""{"contactId":"player_1","journeyId":1,"journeyVersion":1,"journeyStep":"1","acceptUri":"${wireMockServer
                .url(
                  "/please"
                )}","ttl":1,"ttlTimeUnit":"DAY","ruleId":"1","ruleParameters":[{"from":"2023-04-12T08:55:48.693932806Z", "to":"2023-04-11T08:55:48.693932806Z", "kind":"deposit","symbol":"moreOrEqual","times":1,"amount":null,"sports":[]},{"kind":"deposit","symbol":"moreOrEqual","times":null,"amount":33,"sports":[]}]}"""
          )
          .when()
          .post("/application/query")
          .`then`
          .assertThat
          .statusCode(400)
      })

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        given()
          .contentType(ContentType.JSON)
          .body(
            s"""{"contactId":"player_1","journeyId":1,"journeyVersion":1,"journeyStep":"1","acceptUri":"${wireMockServer
                .url(
                  "/please"
                )}","ttl":1,"ttlTimeUnit":"DAY","ruleId":"1","ruleParameters":[{"from":"2023-04-12T08:55:48.693932806Z", "to":"2023-05-14T08:55:48.693932806Z", "kind":"deposit","symbol":"moreOrEqual","times":1,"amount":null,"sports":[]},{"kind":"deposit","symbol":"moreOrEqual","times":null,"amount":33,"sports":[]}]}"""
          )
          .when()
          .post("/application/query")
          .`then`
          .assertThat
          .statusCode(400)
      })
  }
  @Test def shouldJoinWageringAndSportsbets(): Unit = {
    produce(
      config.topicWagering,
      List(
        (
          "key",
          """
        {"bet_id":"88686fe0-52d8-ed11-814a-00155da60c02","player_id":"4d319a9a-0ada-e911-8102-00155d4a2d2c","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":null,"price":"153.99","bet_datetime":"2023-04-11T10:23:26.9982753Z","currency_id":100,"has_resulted":false,"result_datetime":null,"result_id":null} 
      """
        )
      )
    )

    produce(
      config.topicSportsbets,
      List(
        (
          "key",
          """
        {"_time":1681240007.895,"EventTimeStamp":"2023-04-11T19:06:46.6615575Z","bet_id":"88686fe0-52d8-ed11-814a-00155da60c02","player_id":"4d319a9a-0ada-e911-8102-00155d4a2d2c","sport_id":"1265c5bd-d716-e811-80cd-00155d4cf19b","event_id":"39726067","market_id":"de677d0c-e70f-e811-80d9-00155d4cf18a","is_live":false,"championship_id":"e681e7d7-0918-e811-80d5-00155d4cf18c","selection_id":"17e975a6-99d7-ed11-80e5-00155d108919","price":2.47}
      """
        )
      )
    )

    await()
      .atMost(15, TimeUnit.SECONDS)
      .pollDelay(3, TimeUnit.SECONDS)
      .untilAsserted(() => {
        Assertions
          .assertEquals(
            4, // one this test three other one should be improved
            consume(config.topicWageringSport).size
          )
      })

  }
  @Test def shouldCallTheAcceptURI(): Unit = {

    stubFor(post(urlEqualTo("/please")).willReturn(ok()))

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        given()
          .contentType(ContentType.JSON)
          .body(
            s"""{"contactId":"player_1","journeyId":1,"journeyVersion":1,"journeyStep":"1","acceptUri":"${wireMockServer
                .url(
                  "/please"
                )}","ttl":1,"ttlTimeUnit":"DAY","ruleId":"1","ruleParameters":[{"kind":"deposit","symbol":"moreOrEqual","times":"1","amount":33,"sports":[]}]}"""
          )
          .when()
          .post("/application/query")
          .`then`
          .assertThat
          .statusCode(202)
      })

    Thread.sleep(2000)
    produce(
      config.topicWallet,
      List(
        (
          "key",
          """
      {"transaction_status_id":"complete","transaction_id":"transaction_3","transaction_type_id":"deposit-x","player_id":"player_1","amount":"3","transaction_datetime":"1990-03-11T23:47:13.5858638+04:00","currency_id":100}
      """
        ),
        (
          "key",
          """
      {"transaction_status_id":"complete","transaction_id":"transaction_4","transaction_type_id":2,"player_id":"player_1","amount":"4","transaction_datetime":"1990-03-11T19:47:14.5858638Z","currency_id":100}
      """
        ),
        (
          "key",
          """
      {"transaction_status_id":"complete","transaction_id":"transaction_5","transaction_type_id":"deposit-x","player_id":"player_2","amount":"35","transaction_datetime":"1990-03-12T19:47:15.5858638Z","currency_id":100}
      """
        ),
        (
          "key",
          """
      {"transaction_status_id":"complete","transaction_id":"transaction_2","transaction_type_id":"deposit-x","player_id":"player_1","amount":"36","transaction_datetime":"1990-03-11 19:47:16","currency_id":100}
      """
        ),
        (
          "key",
          """
      {"transaction_status_id":"complete","transaction_id":"transaction_1","transaction_type_id":"deposit-x","amount":"37","transaction_datetime":"1990-03-02T19:47:17.5858638Z","currency_id":100}
      """
        )
      )
    )

    produce(
      config.topicPlayer,
      List(
        (
          "key",
          """
    {"_time":1675853008.418,"brand":"betway south africa","dob":"1996-11-14T00:00:00","email":"nope@gmail.com","first_name":"nope","last_name":"micheals","language":"EN","reg_datetime":"1990-03-08T12:43:27.39","player_id":"player_at_0","is_self_excluded":false,"phone_number":"27603658693","country_id":81,"currency_id":100}
    """
        ),
        (
          "key",
          """
    {"_time":1675853008.418,"brand":"betway south africa","dob":"1996-11-14T00:00:00","email":"nope@gmail.com","first_name":"nope","last_name":"micheals","language":"EN","reg_datetime":"1990-03-08T12:43:27.39","player_id":"player_1","is_self_excluded":false,"phone_number":"27603658693","country_id":81,"currency_id":100}
    """
        )
      )
    )

    produce(
      config.topicWageringSport,
      List(
        (
          "key-1",
          """
          {"wagering": {"bet_id":"bet_1","player_id":"player_1","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":null,"price":"153.99","bet_datetime":"1990-03-11T23:47:13.5858638+04:00","currency_id":100,"has_resulted":false,"result_datetime":null,"result_id":null} 
          , "sportsbets": {"_time":1681240007.895,"EventTimeStamp":"2023-04-11T19:06:46.6615575Z","bet_id":"bet_1","player_id":"4d319a9a-0ada-e911-8102-00155d4a2d2c","sport_id":"bjj","event_id":"39726067","market_id":"de677d0c-e70f-e811-80d9-00155d4cf18a","is_live":false,"championship_id":"e681e7d7-0918-e811-80d5-00155d4cf18c","selection_id":"17e975a6-99d7-ed11-80e5-00155d108919","price":2.47}
          }
      """
        ),
        (
          "key-2",
          """
          {"wagering": {"bet_id":"88686fe0-52d8-ed11-814a-00155da60c02","player_id":"player_1","wager_amount_cash":"50.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":null,"price":"153.99","bet_datetime":"1990-03-11T23:48:13.5858638+04:00","currency_id":100,"has_resulted":false,"result_datetime":null,"result_id":null} 
          , "sportsbets": {"_time":1681240007.895,"EventTimeStamp":"2023-04-11T19:06:46.6615575Z","bet_id":"88686fe0-52d8-ed11-814a-00155da60c02","player_id":"4d319a9a-0ada-e911-8102-00155d4a2d2c","sport_id":"handball","event_id":"39726067","market_id":"de677d0c-e70f-e811-80d9-00155d4cf18a","is_live":false,"championship_id":"e681e7d7-0918-e811-80d5-00155d4cf18c","selection_id":"17e975a6-99d7-ed11-80e5-00155d108919","price":2.47}
          }
      """
        ),
        (
          "key-ignored",
          """
          {"wagering": {"bet_id":"bet_1","player_id":"player_1","wager_amount_cash":"5.00","wager_amount_bonus":"0.00","wager_amount_freebet":"0.00","transaction_type_id":3,"ggr_amount":"0","game_id":null,"vertical_id":null,"price":"153.99","bet_datetime":"null","currency_id":100,"has_resulted":false,"result_datetime":null,"result_id":null} 
          , "sportsbets": {"_time":1681240007.895,"EventTimeStamp":"2023-04-11T19:06:46.6615575Z","bet_id":"bet_1","player_id":"4d319a9a-0ada-e911-8102-00155d4a2d2c","sport_id":"bjj","event_id":"39726067","market_id":"de677d0c-e70f-e811-80d9-00155d4cf18a","is_live":false,"championship_id":"e681e7d7-0918-e811-80d5-00155d4cf18c","selection_id":"17e975a6-99d7-ed11-80e5-00155d108919","price":2.47}
          }
      """
        )
      )
    )

    await()
      .atMost(60, TimeUnit.SECONDS)
      .pollDelay(10, TimeUnit.SECONDS)
      .untilAsserted(() => {
        verify(
          1,
          postRequestedFor(urlEqualTo("/please")).withRequestBody(
            equalToJson("""{"properties": {"LastDepositSum": "36.0"}}""")
          )
        )
      })
  }
  @BeforeAll
  def beforeAll(): Unit = {
    val topics = Set(
      config.topicWallet,
      config.topicPlayer,
      config.topicChecker,
      config.topicSport,
      config.topicSportsbets,
      config.topicWagering,
      config.topicWageringSport
    )
    adminClientKafka.deleteTopics(topics.asJavaCollection)
    adminClientKafka.deleteConsumerGroups(
      Set(config.applicationId).asJavaCollection
    )
    adminClientKafka.createTopics(
      topics.map(t => new NewTopic(t, 2, 1.toShort)).asJavaCollection
    )

    wireMockServer = new WireMockServer()
    wireMockServer.start()
  }

  def afterAll: Unit = {
    if (wireMockServer != null) {
      wireMockServer.stop()
    }
  }

  def produce(topic: String, messages: List[(String, String)]): Unit = {
    messages.foreach(value =>
      producer
        .send(
          new ProducerRecord(
            topic,
            value._1,
            value._2
          )
        )
        .get(10, TimeUnit.SECONDS)
    )
  }

  def consume(topic: String): List[(String, String, String)] = {

    val c = getConsumer()
    c.subscribe(Pattern.compile(topic))
    val messages = c
      .poll(java.time.Duration.ofSeconds(5))
      .asScala
      .toList
      .map(consumerRecord => {
        (consumerRecord.topic, consumerRecord.key(), consumerRecord.value())
      })
    messages
  }

  def getConsumer(): KafkaConsumer[String, String] = {
    val prop = new java.util.Properties()
    config.configStreams.forEach((k, v) => prop.put(k, v))
    prop.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer]
    )
    prop.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer]
    )
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    return new KafkaConsumer(
      prop,
      new StringDeserializer(),
      new StringDeserializer()
    )
  }
}
