package rif.simulator

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import rif.catalog.{CloudEvent, CloudEvents, DeadLetter}

import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Duration, Instant}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*

object Kafka:
  val Partitions = 3

  def admin(bootstrap: String): Admin =
    val p = Properties()
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    Admin.create(p)

  def producer(bootstrap: String): KafkaProducer[String, Array[Byte]] =
    val p = Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    KafkaProducer(p, StringSerializer(), ByteArraySerializer())

  def consumer(bootstrap: String, group: String): KafkaConsumer[String, Array[Byte]] =
    val p = Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    KafkaConsumer(p, StringDeserializer(), ByteArrayDeserializer())

  def ensureTopics(bootstrap: String, topics: List[String]): Unit =
    val a = admin(bootstrap)
    try
      val existing = a.listTopics().names().get()
      val missing  = topics.filterNot(existing.contains)
      if missing.nonEmpty then a.createTopics(missing.map(t => NewTopic(t, Partitions, 1.toShort)).asJava).all().get()
    finally a.close()

  /** The record a service writes when it cannot read one: the original bytes and key, and why. */
  def deadLetter(dlq: String, group: String, r: ConsumerRecord[String, Array[Byte]], error: String, at: Instant): ProducerRecord[String, Array[Byte]] =
    val out = ProducerRecord[String, Array[Byte]](dlq, r.key, r.value)
    out.headers()
      .add(DeadLetter.Error, error.getBytes(UTF_8))
      .add(DeadLetter.SourceTopic, r.topic.getBytes(UTF_8))
      .add(DeadLetter.Group, group.getBytes(UTF_8))
      .add(DeadLetter.FailedAt, at.toString.getBytes(UTF_8))
    out

/**
 * A Kafka service: reads its topics in its own consumer group, publishes what `handle` returns, and dead-letters
 * what it cannot read. Offsets are committed after each batch's output is flushed, so consumer lag is real.
 */
final class Service(
    name: String,
    bootstrap: String,
    group: String,
    topics: List[String],
    dlq: String,
    handle: CloudEvent => Either[String, List[Out]]
) extends Runnable:
  @volatile private var running = true
  @volatile private var consumer: Option[KafkaConsumer[String, Array[Byte]]] = None
  val processed    = AtomicLong()
  val deadLettered = AtomicLong()

  def run(): Unit =
    val c = Kafka.consumer(bootstrap, group)
    val p = Kafka.producer(bootstrap)
    consumer = Some(c)
    try
      c.subscribe(topics.asJava)
      while running do
        val records = c.poll(Duration.ofMillis(200))
        for r <- records.asScala do
          CloudEvents.parse(r.value).flatMap(handle) match
            case Right(outs) =>
              outs.foreach(o => p.send(o.record))
              processed.incrementAndGet()
            case Left(error) =>
              p.send(Kafka.deadLetter(dlq, group, r, error, Instant.now()))
              deadLettered.incrementAndGet()
        if !records.isEmpty then
          p.flush()
          c.commitSync()
    catch case _: WakeupException if !running => ()
    finally
      c.close()
      p.close()
      println(s"$name stopped")

  def stop(): Unit =
    running = false
    consumer.foreach(_.wakeup())

  def stats: ujson.Obj = ujson.Obj("processed" -> processed.get.toDouble, "deadLettered" -> deadLettered.get.toDouble)
