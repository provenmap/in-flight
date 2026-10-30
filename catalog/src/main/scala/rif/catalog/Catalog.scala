package rif.catalog

import java.nio.file.{Files, Path}
import scala.util.Try

/** A dotted path into an event's data, such as `total.amount`. */
final case class FieldPath(segments: List[String]):
  def show: String = segments.mkString(".")

  /** Reads this path out of a JSON object. Null counts as missing. */
  def read(json: ujson.Value): Option[ujson.Value] =
    segments.foldLeft(Option(json)) { (acc, name) =>
      acc.flatMap(_.objOpt).flatMap(_.get(name)).filterNot(_.isNull)
    }

object FieldPath:
  def parse(text: String): Either[String, FieldPath] =
    val segments = text.split('.').toList
    if text.nonEmpty && segments.forall(_.nonEmpty) then Right(FieldPath(segments))
    else Left(s"'$text' is not a dotted field path")

/** A place on the map: a team that produces or reads events. A sink, such as the customers, only reads. */
final case class District(id: String, name: String, sink: Boolean)

/** One event type at one version. Versions are immutable once published. */
final case class EventRef(name: String, version: Int):
  def show: String = s"$name@v$version"

/** An event type, its topic and district, and where its value, customer and order are declared to be. */
final case class EventSpec(
    ref: EventRef,
    topic: String,
    district: String,
    value: FieldPath,
    customer: FieldPath,
    order: FieldPath
)

/**
 * A reader of topics. A service has a consumer group and a dead-letter queue; a sink has neither. A service that
 * can redrive its dead-letter queue names the consumer group that does it: that group's committed offsets say
 * which dead letters have been dealt with, which is how the observer knows after a restart.
 */
final case class Consumer(group: Option[String], district: String, reads: List[String], dlq: Option[String], redrive: Option[String] = None):
  def name: String = group.getOrElse(district)

/** The flow: its districts, every event type version, and who reads what. */
final case class Catalog(
    flow: String,
    currency: String,
    districts: List[District],
    events: List[EventSpec],
    consumers: List[Consumer]
):
  def find(ref: EventRef): Option[EventSpec] = events.find(_.ref == ref)
  def district(id: String): Option[District] = districts.find(_.id == id)
  def topics: List[String] = events.map(_.topic).distinct
  def dlqs: List[String] = consumers.flatMap(_.dlq)
  def services: List[Consumer] = consumers.filter(_.group.isDefined)
  def consumer(group: String): Option[Consumer] = consumers.find(_.group.contains(group))
  def readersOf(topic: String): List[Consumer] = consumers.filter(_.reads.contains(topic))

  /** The consumer group that redrives this dead-letter queue, if any. */
  def redriveGroupOf(dlq: String): Option[String] = ownerOfDlq(dlq).flatMap(_.redrive)

  /** The service that dead-letters into this queue. */
  def ownerOfDlq(dlq: String): Option[Consumer] = consumers.find(_.dlq.contains(dlq))

  /** The district that produces onto this topic. One topic belongs to one district. */
  def districtOf(topic: String): Option[String] = events.find(_.topic == topic).map(_.district)

object Catalog:
  val resource = "order-to-cash.json"

  /** The catalog shipped on the classpath. */
  lazy val default: Catalog =
    val in = Option(getClass.getClassLoader.getResourceAsStream(resource))
      .getOrElse(sys.error(s"$resource is not on the classpath"))
    try parse(new String(in.readAllBytes(), "UTF-8")).fold(errs => sys.error(errs.mkString("; ")), identity)
    finally in.close()

  def load(path: Path): Either[List[String], Catalog] = parse(Files.readString(path))

  def parse(text: String): Either[List[String], Catalog] =
    Try(ujson.read(text)).toEither.left.map(e => List(s"not JSON: ${e.getMessage}")).flatMap(fromJson)

  def fromJson(json: ujson.Value): Either[List[String], Catalog] =
    val errors = List.newBuilder[String]

    def field(obj: ujson.Value, key: String): Option[ujson.Value] = obj.objOpt.flatMap(_.get(key))

    def str(obj: ujson.Value, key: String, where: String): String =
      field(obj, key).flatMap(_.strOpt) match
        case Some(s) => s
        case None    => errors += s"$where: '$key' must be a string"; ""

    def path(obj: ujson.Value, key: String, where: String): FieldPath =
      FieldPath.parse(str(obj, key, where)) match
        case Right(p) => p
        case Left(e)  => errors += s"$where: $key: $e"; FieldPath(Nil)

    def arr(obj: ujson.Value, key: String): List[ujson.Value] =
      field(obj, key).flatMap(_.arrOpt).map(_.toList) match
        case Some(a) => a
        case None    => errors += s"'$key' must be an array"; Nil

    val districts = arr(json, "districts").zipWithIndex.map { (d, i) =>
      District(str(d, "id", s"districts[$i]"), str(d, "name", s"districts[$i]"), field(d, "sink").exists(_.boolOpt.contains(true)))
    }

    val events = arr(json, "events").zipWithIndex.map { (e, i) =>
      val where   = s"events[$i]"
      val version = field(e, "version").flatMap(_.numOpt).filter(_.isWhole).map(_.toInt)
      if version.isEmpty then errors += s"$where: 'version' must be a whole number"
      EventSpec(
        EventRef(str(e, "type", where), version.getOrElse(0)),
        str(e, "topic", where),
        str(e, "district", where),
        path(e, "value", where),
        path(e, "customer", where),
        path(e, "order", where)
      )
    }

    val consumers = arr(json, "consumers").zipWithIndex.map { (c, i) =>
      def opt(key: String) = field(c, key).flatMap(_.strOpt)
      val reads = field(c, "reads").flatMap(_.arrOpt).toList.flatten.flatMap(_.strOpt)
      Consumer(opt("group"), str(c, "district", s"consumers[$i]"), reads, opt("dlq"), opt("redrive"))
    }

    val catalog = Catalog(str(json, "flow", "catalog"), str(json, "currency", "catalog"), districts, events, consumers)
    errors ++= validate(catalog)
    val all = errors.result()
    if all.isEmpty then Right(catalog) else Left(all)

  /** Every reference resolves, and no event type version, consumer group or dead-letter queue is declared twice. */
  def validate(c: Catalog): List[String] =
    val ids = c.districts.map(_.id).toSet
    def dupes[A](as: List[A]): List[A] = as.groupBy(identity).collect { case (a, xs) if xs.size > 1 => a }.toList

    val districtErrors = dupes(c.districts.map(_.id)).map(d => s"district '$d' is declared twice")
    val eventErrors = c.events.flatMap { e =>
      Option.when(!ids(e.district))(s"${e.ref.show}: unknown district '${e.district}'") ++
        Option.when(c.district(e.district).exists(_.sink))(s"${e.ref.show}: a sink cannot produce events")
    } ++ dupes(c.events.map(_.ref)).map(r => s"${r.show} is declared twice") ++
      c.events.groupBy(_.topic).collect {
        case (t, es) if es.map(_.district).distinct.size > 1 => s"topic '$t' is produced by more than one district"
      }
    val topics = c.topics.toSet
    val consumerErrors = c.consumers.flatMap { k =>
      Option.when(!ids(k.district))(s"consumer ${k.name}: unknown district '${k.district}'") ++
        Option.when(k.reads.isEmpty)(s"consumer ${k.name}: reads nothing") ++
        k.reads.filterNot(topics).map(t => s"consumer ${k.name}: reads unknown topic '$t'") ++
        Option.when(k.dlq.isDefined && k.group.isEmpty)(s"consumer ${k.name}: a dead-letter queue needs a consumer group") ++
        k.dlq.filter(topics).map(q => s"consumer ${k.name}: dead-letter queue '$q' is also an event topic") ++
        Option.when(k.redrive.isDefined && k.dlq.isEmpty)(s"consumer ${k.name}: a redrive group needs a dead-letter queue")
    } ++ dupes(c.consumers.flatMap(k => k.group ++ k.redrive)).map(g => s"consumer group '$g' is declared twice") ++
      dupes(c.dlqs).map(q => s"dead-letter queue '$q' is declared twice")

    districtErrors ++ eventErrors ++ consumerErrors
