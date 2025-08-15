package com.jada.api

import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import java.time.OffsetDateTime
import java.time.Clock
import java.time.LocalDateTime
import java.time.ZoneOffset
import com.jada.processor.PlayerPartitioner
import java.time.format.DateTimeFormatter

case class PlayerAction(
    val key: String,
    val playerId: String,
    val amount: Double,
    val sportId: Option[String]
)
case class PlayerActionStatus(
    val playerId: String,
    val count: Long,
    val sum: Double
)

@JsonSerialize(`using` = classOf[KindJsonSerializer])
@JsonDeserialize(`using` = classOf[KindJsonDeserializer])
sealed trait Kind
case object Wagered extends Kind
case object Deposit extends Kind

class KindJsonSerializer extends StdSerializer[Kind](classOf[Kind]) {

  override def serialize(
      value: Kind,
      gen: JsonGenerator,
      provider: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case Wagered => "wagered"
      case Deposit => "deposit"
    }
    gen.writeString(strValue)
  }
}

class KindJsonDeserializer extends StdDeserializer[Kind](classOf[Kind]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): Kind = {
    p.getText match {
      case "wagered" => Wagered
      case "deposit" => Deposit
    }
  }
}

@JsonSerialize(`using` = classOf[SymbolJsonSerializer])
@JsonDeserialize(`using` = classOf[SymbolJsonDeserializer])
sealed trait Symbol {

  def compare(base: Double, current: Double): Boolean
}
object More extends Symbol {

  override def compare(base: Double, current: Double): Boolean = current > base

}
object Less extends Symbol {

  override def compare(base: Double, current: Double): Boolean = base < current

}
object Equal extends Symbol {

  override def compare(base: Double, current: Double): Boolean = base == current

}
object MoreOrEqual extends Symbol {

  override def compare(base: Double, current: Double): Boolean = current >= base

}
object LessOrEqual extends Symbol {

  override def compare(base: Double, current: Double): Boolean = current <= base

}

class IntJsonSerializer
    extends StdSerializer[Option[Int]](classOf[Option[Int]]) {

  override def serialize(
      value: Option[Int],
      gen: JsonGenerator,
      provider: SerializerProvider
  ): Unit = {

    value match {
      case Some(v) => gen.writeNumber(v)
      case _       => gen.writeNull()
    }

  }
}

class IntJsonDeserializer
    extends StdDeserializer[Option[Int]](classOf[Option[Int]]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): Option[Int] = {
    val v = p.getValueAsInt(-1)
    if (v == -1) {
      None
    } else {
      Some(v)
    }
  }
}
class SymbolJsonSerializer extends StdSerializer[Symbol](classOf[Symbol]) {

  override def serialize(
      value: Symbol,
      gen: JsonGenerator,
      provider: SerializerProvider
  ): Unit = {
    val strValue = value match {
      case Less        => "less"
      case Equal       => "equal"
      case More        => "more"
      case MoreOrEqual => "moreOrEqual"
      case LessOrEqual => "lessOrEqual"
    }
    gen.writeString(strValue)
  }
}

class SymbolJsonDeserializer extends StdDeserializer[Symbol](classOf[Symbol]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): Symbol = {
    p.getText match {
      case "less" | "<"  => Less
      case "more" | ">"  => More
      case "equal"       => Equal
      case "moreOrEqual" => MoreOrEqual
      case "lessOrEqual" => LessOrEqual
    }
  }
}
case class Rule(
    val kind: Kind,
    val symbol: Symbol,
    @JsonSerialize(`using` = classOf[IntJsonSerializer])
    @JsonDeserialize(`using` = classOf[IntJsonDeserializer])
    val times: Option[Int] = None,
    val amount: Option[String] = None,
    val sports: List[String] = List.empty,
    val from: Option[OffsetDateTime],
    val to: Option[OffsetDateTime]
) {

  def accept(value: PlayerActionStatus): Boolean = {
    acceptTimesAmount(value)
  }

  def accept(value: PlayerAction): Boolean = {
    this.amount.map(v => symbol.compare(v.toInt, value.amount)).getOrElse(false)
  }

  def acceptTimesAmount(value: PlayerActionStatus): Boolean = {
    this.times.map(symbol.compare(_, value.count)).getOrElse(true) && // times
    this.amount
      .map(v => symbol.compare(v.toInt, value.sum))
      .getOrElse(true) && // amount
    (this.times.isDefined || this.amount.isDefined) // at least one rule has been defined
  }

  def handle(
      contactId: String,
      key: String,
      sportId: Option[String]
  ): Boolean = {
    (key >= s"${contactId}${PlayerPartitioner.Separator}${from
        .map(_.withOffsetSameInstant(ZoneOffset.UTC).withNano(0))
        .get
        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}${PlayerPartitioner.Separator}${PlayerPartitioner.UUIDFrom}" &&
      key <= s"${contactId}${PlayerPartitioner.Separator}${to
          .map(_.withOffsetSameInstant(ZoneOffset.UTC).withNano(0))
          .get
          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}${PlayerPartitioner.Separator}${PlayerPartitioner.UUIDTo}") &&
    (sportId.isEmpty || sports.isEmpty || sports.contains(sportId.get))
  }

}

case class AggregatorRequest(
    val contactId: String,
    val journeyId: Int,
    val journeyVersion: Int,
    val journeyStep: String,
    val acceptUri: String,
    val ttl: Int,
    val ttlTimeUnit: String,
    val ruleId: String,
    val ruleParameters: List[Rule],
    val from: Option[OffsetDateTime]
) {

  def withTtl(): Option[java.time.OffsetDateTime] =
    from
      .map(d =>
        ttlTimeUnit.toUpperCase() match {
          case "SECOND" | "SECONDS" => d.plusSeconds(ttl)
          case "MINUTE" | "MINUTES" => d.plusMinutes(ttl)
          case "HOUR" | "HOURS"     => d.plusHours(ttl)
          case "DAY" | "DAYS"       => d.plusDays(ttl)
          case "WEEK" | "WEEKS"     => d.plusWeeks(ttl)
          case "MONTH" | "MONTHS"   => d.plusMonths(ttl)
          case "YEAR" | "YEARS"     => d.plusYears(ttl)
          case _                    => null
        }
      )
      .filter(_ != null)

  def expired(clock: Clock): Boolean = {
    withTtl
      .map(to => {
        val now = LocalDateTime.now(clock).atOffset(ZoneOffset.UTC)
        now.isAfter(to)
      })
      .getOrElse(true) // getOrElse at true it means the from date is missing
  }
}
