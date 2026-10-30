package rif.simulator

import org.apache.kafka.clients.producer.ProducerRecord
import rif.catalog.{CloudEvent, Pricing}

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.math.BigDecimal.RoundingMode

/** An event to publish, keyed by its order so one order's events stay in one partition. */
final case class Out(topic: String, key: String, event: CloudEvent):
  def record: ProducerRecord[String, Array[Byte]] = ProducerRecord(topic, key, event.json.getBytes(UTF_8))

/** Stamps events from one source with a fresh id and that source's own sequence. */
final class Emitter(source: String):
  private val seq = AtomicLong()

  def apply(tpe: String, version: Int, at: Instant, data: ujson.Obj, redriveOf: Option[String] = None): CloudEvent =
    CloudEvent(UUID.randomUUID().toString, source, tpe, version, at, seq.incrementAndGet(), data, redriveOf)

/** Reading fields the way a service would: by name, failing with the reason it would log. */
object Read:
  def str(data: ujson.Value, key: String): Either[String, String] =
    field(data, key).flatMap(v => v.strOpt.toRight(s"'$key' is ${kind(v)}, expected a string"))

  /** A decimal string, the way every amount in this flow was written before v2. */
  def decimal(data: ujson.Value, key: String): Either[String, BigDecimal] =
    field(data, key).flatMap { v =>
      v.strOpt.toRight(s"'$key' is ${kind(v)}, expected a decimal string")
        .flatMap(s => Pricing.money(ujson.Str(s)).toRight(s"'$key' is '$s', not a decimal"))
    }

  def field(data: ujson.Value, key: String): Either[String, ujson.Value] =
    data.objOpt.flatMap(_.get(key)).filterNot(_.isNull).toRight(s"no '$key'")

  def kind(v: ujson.Value): String = v match
    case _: ujson.Obj  => "an object"
    case _: ujson.Arr  => "an array"
    case _: ujson.Str  => "a string"
    case _: ujson.Num  => "a number"
    case _: ujson.Bool => "a boolean"
    case _             => "null"

/** Amounts on the wire: two decimal places, as a string. */
def money(b: BigDecimal): String = b.setScale(2, RoundingMode.HALF_UP).toString
