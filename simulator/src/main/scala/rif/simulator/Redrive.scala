package rif.simulator

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import rif.catalog.{CloudEvent, CloudEvents}

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** How the last redrive went. */
final case class RedriveStatus(running: Boolean, redriven: Long, remaining: Long, error: Option[String]):
  def json: ujson.Obj =
    ujson.Obj("running" -> running, "redriven" -> redriven.toDouble, "remaining" -> remaining.toDouble,
              "error" -> error.fold[ujson.Value](ujson.Null)(ujson.Str(_)))

object RedriveStatus:
  val idle = RedriveStatus(false, 0, 0, None)

/**
 * One pass over a dead-letter queue, up to the end it had when the pass began, in its own consumer group. Each
 * record goes through `handle` again; what it returns is published carrying the original event id. The first
 * record that still fails stops its partition, and the committed offset stays on it, so a later pass retries it.
 * Nothing is re-published to the source topic, so the other readers of that topic never see an event twice.
 */
final class Redrive(bootstrap: String, dlq: String, group: String, handle: CloudEvent => Either[String, List[Out]]):

  def run(progress: RedriveStatus => Unit): RedriveStatus =
    val c = Kafka.consumer(bootstrap, group)
    val p = Kafka.producer(bootstrap)
    try
      val parts = c.partitionsFor(dlq).asScala.map(i => TopicPartition(dlq, i.partition)).toList
      c.assign(parts.asJava)
      val end       = c.endOffsets(parts.asJava).asScala.map((tp, o) => tp -> o.longValue).toMap
      val committed = c.committed(parts.toSet.asJava).asScala
      parts.foreach { tp =>
        Option(committed.get(tp).orNull).fold(c.seekToBeginning(List(tp).asJava))(o => c.seek(tp, o.offset))
      }
      val next   = mutable.Map.from(parts.map(tp => tp -> c.position(tp)))
      val open   = mutable.Set.from(parts.filter(tp => next(tp) < end(tp)))
      var done   = 0L
      var error  = Option.empty[String]
      val giveUp = Instant.now().plusSeconds(60)
      def remaining = parts.map(tp => end(tp) - next(tp)).sum

      while open.nonEmpty && Instant.now().isBefore(giveUp) do
        val commits = mutable.Map.empty[TopicPartition, OffsetAndMetadata]
        for r <- c.poll(Duration.ofMillis(200)).asScala do
          val tp = TopicPartition(r.topic, r.partition)
          if open(tp) && r.offset < end(tp) then
            CloudEvents.parse(r.value).flatMap(handle) match
              case Right(outs) =>
                outs.foreach(o => p.send(o.record))
                next(tp) = r.offset + 1
                commits(tp) = OffsetAndMetadata(r.offset + 1)
                done += 1
                if next(tp) >= end(tp) then open -= tp
              case Left(e) =>
                error = Some(s"offset ${r.offset} on partition ${r.partition}: $e")
                c.pause(List(tp).asJava)
                open -= tp
        if commits.nonEmpty then
          p.flush()
          c.commitSync(commits.asJava)
        progress(RedriveStatus(true, done, remaining, error))
      RedriveStatus(false, done, remaining, error)
    finally
      c.close()
      p.close()
