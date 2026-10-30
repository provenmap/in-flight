package rif.observer

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, OffsetSpec}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import rif.catalog.{Catalog, DeadLetter, Pricing}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration, Instant}
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** What the poller last saw: each service group's committed offsets, each partition's end, and the simulator's state. */
final case class Polled(
    committed: Map[String, Map[(String, Int), Long]],
    ends: Map[(String, Int), Long],
    sim: Option[ujson.Value]
)

object Kafka:
  def admin(bootstrap: String): Admin =
    val p = Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    Admin.create(p)

  /** A reader outside any consumer group: it commits nothing and shows up in nobody's lag. */
  def reader(bootstrap: String): KafkaConsumer[String, Array[Byte]] =
    val p = Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    KafkaConsumer(p, StringDeserializer(), ByteArrayDeserializer())

  def committed(admin: Admin, group: String): Map[(String, Int), Long] =
    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().asScala.collect {
      case (tp, om) if om != null => (tp.topic, tp.partition) -> om.offset
    }.toMap

/**
 * Polls, once a second, what the reader cannot see in the records themselves: the service groups' committed
 * offsets, the end of every partition they read, and the simulator's knobs.
 */
final class Poller(bootstrap: String, catalog: Catalog, simulator: String, latest: AtomicReference[Option[Polled]]) extends Runnable:
  @volatile private var running = true
  private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build()

  def run(): Unit =
    val admin = Kafka.admin(bootstrap)
    try
      while running do
        Try {
          val groups = catalog.services.flatMap(_.group).map(g => g -> Kafka.committed(admin, g)).toMap
          val topics = catalog.services.flatMap(_.reads).distinct
          val parts  = admin.describeTopics(topics.asJava).allTopicNames().get().asScala.toList.flatMap { (t, d) =>
            d.partitions.asScala.map(p => TopicPartition(t, p.partition))
          }
          val ends = admin.listOffsets(parts.map(_ -> OffsetSpec.latest()).toMap.asJava).all().get().asScala
            .map((tp, r) => (tp.topic, tp.partition) -> r.offset).toMap
          latest.set(Some(Polled(groups, ends, simState())))
        }.failed.foreach(e => System.err.println(s"poller: ${e.getMessage}"))
        Thread.sleep(1000)
    catch case _: InterruptedException => ()
    finally admin.close()

  private def simState(): Option[ujson.Value] =
    Try {
      val req = HttpRequest.newBuilder(URI.create(s"$simulator/state")).timeout(Duration.ofSeconds(1)).GET().build()
      ujson.read(http.send(req, HttpResponse.BodyHandlers.ofString()).body)
    }.toOption

  def stop(): Unit = running = false

/**
 * Reads every dead-letter queue from the start and every flow topic from the end, prices each record, and feeds the
 * ledger. It owns the ledger: stats and snapshots for new viewers are built on this thread too.
 */
final class Reader(
    bootstrap: String,
    catalog: Catalog,
    publish: List[ujson.Obj] => Unit,
    snapshots: LinkedBlockingQueue[ujson.Obj => Unit],
    latest: AtomicReference[Option[Polled]]
) extends Runnable:
  @volatile private var running = true
  private val ledger  = Ledger(catalog)
  private val pricing = Pricing(catalog)
  private val dlqs    = catalog.dlqs.toSet

  def run(): Unit =
    val admin = Kafka.admin(bootstrap)
    val c     = Kafka.reader(bootstrap)
    try
      val topics = catalog.topics ++ catalog.dlqs
      waitFor(admin, topics)
      for dlq <- catalog.dlqs; group <- catalog.redriveGroupOf(dlq); ((t, p), o) <- Kafka.committed(admin, group) if t == dlq do
        ledger.redriven(dlq, p, o)
      val parts = topics.flatMap(t => c.partitionsFor(t).asScala.map(i => TopicPartition(t, i.partition)))
      c.assign(parts.asJava)
      c.seekToBeginning(parts.filter(tp => dlqs(tp.topic)).asJava)
      c.seekToEnd(parts.filterNot(tp => dlqs(tp.topic)).asJava)
      parts.foreach(c.position) // pin the end now, so nothing produced from here on is skipped
      println(s"observer: reading ${catalog.dlqs.mkString(", ")} from the start and ${catalog.topics.size} flow topics from the end")

      var seen      = Option.empty[Polled]
      var lastStats = Instant.EPOCH
      while running do
        val msgs = c.poll(Duration.ofMillis(100)).asScala.toList.flatMap(read)
        val polled = latest.get
        if polled.isDefined && polled != seen then
          seen = polled
          polled.foreach(p => p.committed.foreach((g, offs) => ledger.onOffsets(g, offs, p.ends)))
        val now = Instant.now()
        val stats =
          if Duration.between(lastStats, now).toMillis >= 500 then
            lastStats = now
            List(statsJson)
          else Nil
        if msgs.nonEmpty || stats.nonEmpty then publish(msgs.map(_.json) ++ stats)
        Iterator.continually(snapshots.poll()).takeWhile(_ != null).foreach(_(snapshot))
    catch case _: org.apache.kafka.common.errors.WakeupException if !running => ()
    finally
      c.close()
      admin.close()

  def stop(): Unit = running = false

  private def read(r: ConsumerRecord[String, Array[Byte]]): List[Msg] =
    val pos = Pos(r.topic, r.partition, r.offset)
    pricing.price(r.value) match
      case Left(_) =>
        ledger.unreadable()
        Nil
      case Right(priced) if dlqs(r.topic) =>
        def header(k: String) = Option(r.headers.lastHeader(k)).map(h => String(h.value, UTF_8))
        val at = header(DeadLetter.FailedAt).flatMap(t => Try(Instant.parse(t)).toOption).getOrElse(Instant.ofEpochMilli(r.timestamp))
        List(ledger.onDead(r.topic, pos, priced, header(DeadLetter.Error).getOrElse("unknown"), at))
      case Right(priced) =>
        ledger.onFlow(pos, priced, Instant.ofEpochMilli(r.timestamp))

  private def statsJson: ujson.Obj =
    val s = ledger.stats.json
    s("sim") = latest.get.flatMap(_.sim).getOrElse(ujson.Null)
    s

  private def snapshot: ujson.Obj =
    val stuck = ledger.stuckNow
    ujson.Obj(
      "type" -> "snapshot",
      "flow" -> Snapshot.flow(catalog),
      "stuck" -> ujson.Arr.from(stuck.takeRight(Snapshot.MaxStuck).map(_.json)),
      "stuckShown" -> math.min(stuck.size, Snapshot.MaxStuck),
      "stats" -> statsJson
    )

  private def waitFor(admin: Admin, topics: List[String]): Unit =
    var told = false
    while running && !topics.forall(admin.listTopics().names().get().contains) do
      if !told then println(s"observer: waiting for topics ${topics.mkString(", ")} (start the simulator)")
      told = true
      Thread.sleep(2000)

object Snapshot:
  /** A viewer cannot show more red blocks than this usefully; the counter still counts them all. */
  val MaxStuck = 3000

  /** The map: districts, and each flow topic with where it comes from and who reads it. */
  def flow(c: Catalog): ujson.Obj = ujson.Obj(
    "name" -> c.flow,
    "currency" -> c.currency,
    "districts" -> ujson.Arr.from(c.districts.map(d => ujson.Obj("id" -> d.id, "name" -> d.name, "sink" -> d.sink))),
    "topics" -> ujson.Arr.from(c.topics.map { t =>
      ujson.Obj("topic" -> t, "from" -> c.districtOf(t).getOrElse(""), "to" -> c.readersOf(t).map(_.district),
                "events" -> c.events.filter(_.topic == t).map(_.ref.show))
    }),
    "dlqs" -> ujson.Arr.from(c.services.flatMap(k => k.dlq.map(q => ujson.Obj("dlq" -> q, "district" -> k.district, "group" -> k.name))))
  )
