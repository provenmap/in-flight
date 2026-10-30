package rif.catalog

import java.time.Instant
import java.util.UUID
import scala.util.Try

/** A structured-mode CloudEvent, with the extensions this flow reads: `schemaversion`, `sequence` and `redriveof`. */
final case class CloudEvent(
    id: String,
    source: String,
    eventType: String,
    version: Int,
    time: Instant,
    sequence: Long,
    data: ujson.Value,
    redriveOf: Option[String] = None
):
  def ref: EventRef = EventRef(eventType, version)

  /** The JSON form, as it goes on the wire. */
  def json: String =
    val obj = ujson.Obj(
      "specversion" -> "1.0",
      "id" -> id,
      "source" -> source,
      "type" -> eventType,
      "time" -> time.toString,
      "datacontenttype" -> "application/json",
      "schemaversion" -> version,
      "sequence" -> sequence.toString,
      "data" -> data
    )
    redriveOf.foreach(o => obj("redriveof") = o)
    ujson.write(obj)

object CloudEvents:
  /** A new event with a fresh id. */
  def apply(source: String, eventType: String, version: Int, time: Instant, sequence: Long, data: ujson.Obj): CloudEvent =
    CloudEvent(UUID.randomUUID().toString, source, eventType, version, time, sequence, data)

  def parse(bytes: Array[Byte]): Either[String, CloudEvent] = parse(new String(bytes, "UTF-8"))

  def parse(text: String): Either[String, CloudEvent] =
    Try(ujson.read(text)).toEither.left.map(e => s"not JSON: ${e.getMessage}").flatMap { json =>
      def str(key: String) = json.objOpt.flatMap(_.get(key)).flatMap(_.strOpt)
      def need(key: String) = str(key).toRight(s"CloudEvent has no '$key'")
      val version = json.objOpt.flatMap(_.get("schemaversion")).flatMap(v => v.numOpt.orElse(v.strOpt.flatMap(_.toDoubleOption)))
      for
        id      <- need("id")
        source  <- need("source")
        tpe     <- need("type")
        v       <- version.filter(_.isWhole).map(_.toInt).toRight(s"$tpe ($id) has no whole schemaversion")
        data    <- json.objOpt.flatMap(_.get("data")).filter(_.objOpt.isDefined).toRight(s"$tpe ($id) has no data object")
        time     = str("time").flatMap(t => Try(Instant.parse(t)).toOption).getOrElse(Instant.EPOCH)
        sequence = str("sequence").flatMap(_.toLongOption).getOrElse(0L)
      yield CloudEvent(id, source, tpe, v, time, sequence, data, str("redriveof"))
    }

/** Headers a service sets when it dead-letters a record. */
object DeadLetter:
  val Error       = "rif-error"
  val SourceTopic = "rif-source-topic"
  val Group       = "rif-consumer-group"
  val FailedAt    = "rif-failed-at"
