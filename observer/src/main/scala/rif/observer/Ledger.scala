package rif.observer

import rif.catalog.{Catalog, Priced}

import java.time.{Duration, Instant}
import scala.collection.mutable

/** A record's place in a topic. */
final case class Pos(topic: String, partition: Int, offset: Long)

/** A dead-lettered event: where it sits, what it is worth, and why it could not be read. */
final case class Stuck(id: String, dlq: String, district: String, pos: Pos, priced: Priced, error: String, at: Instant):
  def json: ujson.Obj = ujson.Obj(
    "type" -> "dead", "id" -> id, "dlq" -> dlq, "district" -> district, "event" -> priced.ref.show,
    "value" -> priced.value.toDouble, "customer" -> priced.customer, "orderId" -> priced.orderId,
    "error" -> error, "at" -> at.toString
  )

/** One message for the view. */
enum Msg:
  case Event(priced: Priced, to: List[String])
  case Dead(stuck: Stuck)
  /** `after` is unknown for a dead letter found already redriven when the observer started. */
  case Recovered(stuck: Stuck, after: Option[Duration])

  def json: ujson.Obj = this match
    case Event(p, to) => ujson.Obj(
      "type" -> "event", "id" -> p.eventId, "event" -> p.ref.show, "topic" -> p.topic, "district" -> p.district,
      "to" -> to, "value" -> p.value.toDouble, "customer" -> p.customer, "orderId" -> p.orderId,
      "redrive" -> p.redriveOf.isDefined)
    case Dead(s) => s.json
    case Recovered(s, after) => ujson.Obj(
      "type" -> "recovered", "id" -> s.id, "district" -> s.district, "value" -> s.priced.value.toDouble,
      "afterMs" -> after.fold[ujson.Value](ujson.Null)(d => ujson.Num(d.toMillis.toDouble)))

/** Consumer lag of one group: how many events it has not committed, and what the ones the observer saw are worth. */
final case class GroupLag(events: Long, value: BigDecimal, customers: Int)

/** A stuck spell: from the first event stuck while none were, to the last recovery that emptied the pile. */
final case class Episode(value: BigDecimal, events: Int, took: Duration)

final case class Stats(
    stuckValue: BigDecimal,
    stuckEvents: Int,
    stuckCustomers: Int,
    stuckSince: Option[Instant],
    stuckByDistrict: Map[String, BigDecimal],
    lag: Map[String, GroupLag],
    recoveredValue: BigDecimal,
    recoveredEvents: Int,
    atRiskValue: BigDecimal,
    atRiskOrders: Int,
    atRiskCustomers: Int,
    lastEpisode: Option[Episode],
    unpriced: Long
):
  def lagValue: BigDecimal = lag.values.map(_.value).sum
  def lagEvents: Long      = lag.values.map(_.events).sum

  def json: ujson.Obj = ujson.Obj(
    "type" -> "stats",
    "atRisk" -> ujson.Obj("value" -> atRiskValue.toDouble, "orders" -> atRiskOrders, "customers" -> atRiskCustomers),
    "stuck" -> ujson.Obj("value" -> stuckValue.toDouble, "events" -> stuckEvents, "customers" -> stuckCustomers,
      "since" -> stuckSince.fold[ujson.Value](ujson.Null)(t => ujson.Str(t.toString)),
      "byDistrict" -> ujson.Obj.from(stuckByDistrict.toList.sortBy(_._1).map((d, v) => d -> ujson.Num(v.toDouble)))),
    "lag" -> ujson.Obj("value" -> lagValue.toDouble, "events" -> lagEvents.toDouble,
      "byGroup" -> ujson.Obj.from(lag.toList.sortBy(_._1).map((g, l) =>
        g -> ujson.Obj("events" -> l.events.toDouble, "value" -> l.value.toDouble, "customers" -> l.customers)))),
    "recovered" -> ujson.Obj("value" -> recoveredValue.toDouble, "events" -> recoveredEvents),
    "lastEpisode" -> lastEpisode.fold[ujson.Value](ujson.Null)(e =>
      ujson.Obj("value" -> e.value.toDouble, "events" -> e.events, "tookMs" -> e.took.toMillis.toDouble)),
    "unpriced" -> unpriced.toDouble
  )

/**
 * Money at risk, from what the observer has read. Not thread-safe: one thread feeds it and asks it for stats.
 *
 *  - Stuck: every dead letter not yet recovered.
 *  - Lag: for each service's group, the events it has not committed that the observer saw arrive.
 *  - Recovered: a flow event whose `redriveof` names a stuck one, or a dead letter below its redrive group's
 *    committed offset when the observer started.
 *
 * The headline counts each order once, at the most it is worth anywhere it is held up, and each customer once.
 */
final class Ledger(catalog: Catalog, window: Int = 50000):
  private val stuck          = mutable.LinkedHashMap.empty[String, Stuck]
  private val recoveredIds   = mutable.Set.empty[String]
  private val redrivenBelow  = mutable.Map.empty[(String, Int), Long]
  private val recent         = mutable.Map.empty[(String, Int), mutable.ArrayDeque[(Long, Priced)]]
  private val committed      = mutable.Map.empty[String, Map[(String, Int), Long]]
  private val ends           = mutable.Map.empty[(String, Int), Long]
  private var recoveredValue = BigDecimal(0)
  private var recoveredCount = 0
  private var unpriced       = 0L
  private var episodeStart   = Option.empty[Instant]
  private var episodeValue   = BigDecimal(0)
  private var episodeCount   = 0
  private var lastEpisode    = Option.empty[Episode]

  /** Topics a service group reads: only those need their recent events remembered for lag. */
  private val tracked = catalog.services.flatMap(_.reads).toSet

  /** On start: the redrive group of `dlq` has dealt with everything on `partition` below `offset`. */
  def redriven(dlq: String, partition: Int, offset: Long): Unit = redrivenBelow((dlq, partition)) = offset

  def unreadable(): Unit = unpriced += 1

  def onDead(dlq: String, pos: Pos, priced: Priced, error: String, failedAt: Instant): Msg =
    val district = catalog.ownerOfDlq(dlq).map(_.district).getOrElse(priced.district)
    val s        = Stuck(priced.eventId, dlq, district, pos, priced, error, failedAt)
    if pos.offset < redrivenBelow.getOrElse((dlq, pos.partition), 0L) || recoveredIds(s.id) then
      recover(s, None)
    else
      if stuck.isEmpty then
        episodeStart = Some(failedAt)
        episodeValue = 0
        episodeCount = 0
      stuck(s.id) = s
      Msg.Dead(s)

  def onFlow(pos: Pos, priced: Priced, at: Instant): List[Msg] =
    if tracked(pos.topic) then
      val q = recent.getOrElseUpdate((pos.topic, pos.partition), mutable.ArrayDeque.empty)
      q.append(pos.offset -> priced)
      while q.size > window do q.removeHead()
    val event = Msg.Event(priced, catalog.readersOf(pos.topic).map(_.district))
    val recovered = priced.redriveOf.toList.flatMap { orig =>
      stuck.remove(orig) match
        case Some(s) =>
          val m = recover(s, Some(Duration.between(s.at, at)))
          episodeValue += s.priced.value
          episodeCount += 1
          if stuck.isEmpty then
            lastEpisode = episodeStart.map(st => Episode(episodeValue, episodeCount, Duration.between(st, at)))
            episodeStart = None
          List(m)
        case None =>
          // The redriven event overtook its dead letter: remember it, so the dead letter arrives recovered.
          recoveredIds += orig
          Nil
    }
    event :: recovered

  private def recover(s: Stuck, after: Option[Duration]): Msg =
    recoveredValue += s.priced.value
    recoveredCount += 1
    recoveredIds -= s.id
    Msg.Recovered(s, after)

  /** The latest committed offsets of a service group, and the end of each partition. */
  def onOffsets(group: String, offsets: Map[(String, Int), Long], logEnds: Map[(String, Int), Long]): Unit =
    committed(group) = offsets
    ends ++= logEnds
    // Forget events every group reading their topic has committed.
    for ((topic, part), q) <- recent do
      val floors = catalog.services.filter(_.reads.contains(topic)).flatMap(_.group).map(g => committed.get(g).flatMap(_.get((topic, part))))
      if floors.nonEmpty && floors.forall(_.isDefined) then
        val floor = floors.flatten.min
        while q.headOption.exists(_._1 < floor) do q.removeHead()

  /**
   * Events a group has not committed, with the ones the observer saw, between its committed offset and the end
   * polled with it. Events newer than that end are in flight, not yet lag, so the count and the value agree.
   */
  private def lagOf(group: String): (Long, List[Priced]) =
    val reads = catalog.consumer(group).toList.flatMap(_.reads).toSet
    val offs  = committed.getOrElse(group, Map.empty)
    val parts = ends.keys.filter((t, _) => reads(t)).toList
    val count = parts.map(tp => math.max(0L, ends(tp) - offs.getOrElse(tp, ends(tp)))).sum
    val held  = parts.flatMap { tp =>
      val (from, end) = (offs.getOrElse(tp, Long.MaxValue), ends(tp))
      recent.get(tp).toList.flatMap(_.collect { case (o, p) if o >= from && o < end => p })
    }
    (count, held)

  def stuckNow: List[Stuck] = stuck.values.toList

  def stats: Stats =
    val held   = stuck.values.toList
    val groups = catalog.services.flatMap(_.group).map(g => g -> lagOf(g))
    val lagged = groups.flatMap(_._2._2)
    val byOrder = (held.map(_.priced) ++ lagged).groupMapReduce(_.orderId)(identity)((a, b) => if a.value >= b.value then a else b)
    Stats(
      stuckValue = held.map(_.priced.value).sum,
      stuckEvents = held.size,
      stuckCustomers = held.map(_.priced.customer).distinct.size,
      stuckSince = held.map(_.at).minOption,
      stuckByDistrict = held.groupMapReduce(_.district)(_.priced.value)(_ + _),
      lag = groups.map((g, l) => g -> GroupLag(l._1, l._2.map(_.value).sum, l._2.map(_.customer).distinct.size)).toMap,
      recoveredValue = recoveredValue,
      recoveredEvents = recoveredCount,
      atRiskValue = byOrder.values.map(_.value).sum,
      atRiskOrders = byOrder.size,
      atRiskCustomers = byOrder.values.map(_.customer).toSet.size,
      lastEpisode = lastEpisode,
      unpriced = unpriced
    )
