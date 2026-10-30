package rif.simulator

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import rif.catalog.Catalog

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{Executors, ThreadLocalRandom}
import scala.util.Random

/** The knobs the control API turns. */
final class Controls:
  @volatile var orderVersion: Int = 1
  @volatile var patched: Boolean  = false
  @volatile var rate: Double      = 8.0
  val redrive = AtomicReference(RedriveStatus.idle)

/** The orders team at work: orders arrive as a Poisson process at `rate` a second, in the current schema version. */
final class Traffic(bootstrap: String, controls: Controls) extends Runnable:
  @volatile private var running = true
  val placed = AtomicLong()

  def run(): Unit =
    val p    = Kafka.producer(bootstrap)
    val emit = Emitter("/orders")
    val rng  = Random()
    try
      while running do
        val rate = controls.rate
        if rate <= 0 then Thread.sleep(200)
        else
          p.send(Orders.placed(emit, rng, controls.orderVersion, Instant.now()).record)
          placed.incrementAndGet()
          val gapMs = -math.log(1 - rng.nextDouble()) / rate * 1000
          Thread.sleep(math.min(gapMs, 5000).toLong)
    catch case _: InterruptedException => ()
    finally p.close()

  def stop(): Unit = running = false

/**
 * Order traffic and the three services, with a control API on CONTROL_PORT (8090):
 *
 * {{{
 * GET  /state                   the knobs, each service's counts and the last redrive
 * POST /break | /restore        orders switch to OrderPlaced v2 | back to v1
 * POST /patch | /unpatch        payments reads both shapes | only v1
 * POST /redrive                 one pass over payments.dlq with the current payments code
 * POST /rate?perSecond=N        order traffic
 * }}}
 */
object Main:
  def main(args: Array[String]): Unit =
    val bootstrap = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
    val port      = sys.env.get("CONTROL_PORT").map(_.toInt).getOrElse(8090)
    val catalog   = Catalog.default
    Kafka.ensureTopics(bootstrap, catalog.topics ++ catalog.dlqs)

    val controls  = Controls()
    val traffic   = Traffic(bootstrap, controls)
    val paymentsE = Emitter("/payments")
    val payments  = Service("payments-service", bootstrap, Payments.Group, List(Orders.Topic), Payments.Dlq, e =>
      // An external card authorisation takes a few milliseconds; a flood of orders builds real lag.
      Thread.sleep(10 + ThreadLocalRandom.current().nextInt(15))
      Payments.handle(paymentsE, e, controls.patched, Instant.now()))
    val inventoryE = Emitter("/inventory")
    val inventory  = Service("inventory-service", bootstrap, Inventory.Group, List(Orders.Topic), Inventory.Dlq, e =>
      Inventory.handle(inventoryE, e, Instant.now()))
    val shipping  = Shipping(Emitter("/shipping"))
    val shipper   = Service("shipping-service", bootstrap, Shipping.Group, Shipping.Reads, Shipping.Dlq, e =>
      shipping.handle(e, Instant.now()))
    val services  = List("payments" -> payments, "inventory" -> inventory, "shipping" -> shipper)

    val redriveGroup = catalog.redriveGroupOf(Payments.Dlq).getOrElse(sys.error(s"the catalog names no redrive group for ${Payments.Dlq}"))
    val redrive = Redrive(bootstrap, Payments.Dlq, redriveGroup, e =>
      Payments.handle(paymentsE, e, controls.patched, Instant.now(), redrive = true))
    val redriver = Executors.newSingleThreadExecutor()

    val threads = (traffic :: services.map(_._2)).map(r => Thread(r))
    threads.foreach(_.start())

    def state: ujson.Obj = ujson.Obj(
      "orderVersion" -> controls.orderVersion,
      "patched" -> controls.patched,
      "rate" -> controls.rate,
      "placed" -> traffic.placed.get.toDouble,
      "services" -> ujson.Obj.from(services.map((n, s) => n -> s.stats)),
      "shippingWaiting" -> shipping.pending,
      "redrive" -> controls.redrive.get.json
    )

    def startRedrive(): Either[String, Unit] =
      val idle = controls.redrive.get
      if idle.running then Left("a redrive is already running")
      else
        controls.redrive.set(RedriveStatus(true, 0, 0, None))
        redriver.submit((() =>
          val result =
            try redrive.run(controls.redrive.set)
            catch case e: Exception => RedriveStatus(false, 0, 0, Some(e.toString))
          controls.redrive.set(result)
          println(s"redrive: ${result.redriven} redriven, ${result.remaining} remaining${result.error.fold("")(e => s", stopped at $e")}")
        ): Runnable)
        Right(())

    val server = HttpServer.create(InetSocketAddress(port), 0)
    server.createContext("/", (ex: HttpExchange) =>
      val path   = ex.getRequestURI.getPath
      val query  = Option(ex.getRequestURI.getQuery).toList.flatMap(_.split('&')).map(_.split('=')).collect { case Array(k, v) => k -> v }.toMap
      val method = ex.getRequestMethod
      val result: Either[(Int, String), Unit] = (method, path) match
        case ("GET", "/state")     => Right(())
        case ("POST", "/break")    => Right(controls.orderVersion = 2)
        case ("POST", "/restore")  => Right(controls.orderVersion = 1)
        case ("POST", "/patch")    => Right(controls.patched = true)
        case ("POST", "/unpatch")  => Right(controls.patched = false)
        case ("POST", "/redrive")  => startRedrive().left.map(409 -> _)
        case ("POST", "/rate") =>
          query.get("perSecond").flatMap(_.toDoubleOption).filter(r => r >= 0 && r <= 500)
            .toRight(400 -> "rate needs perSecond between 0 and 500").map(r => controls.rate = r)
        case (_, "/state" | "/break" | "/restore" | "/patch" | "/unpatch" | "/redrive" | "/rate") => Left(405 -> s"$method not allowed")
        case _ => Left(404 -> s"no such endpoint $path")
      if method == "POST" && result.isRight then println(s"control: $path ${query.mkString(" ")}".trim)
      val (code, body) = result.fold((c, msg) => (c, ujson.Obj("error" -> msg)), _ => (200, state))
      val bytes = ujson.write(body).getBytes(UTF_8)
      ex.getResponseHeaders.set("Content-Type", "application/json")
      ex.sendResponseHeaders(code, bytes.length)
      ex.getResponseBody.write(bytes)
      ex.close()
    )
    server.start()
    println(s"simulator: Kafka at $bootstrap, ${controls.rate} orders/s, control API on http://localhost:$port")

    sys.addShutdownHook {
      server.stop(0)
      traffic.stop()
      services.foreach(_._2.stop())
      redriver.shutdownNow()
      threads.foreach(_.join(5000))
    }
