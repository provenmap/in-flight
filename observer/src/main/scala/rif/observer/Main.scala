package rif.observer

import io.javalin.Javalin
import io.javalin.http.staticfiles.Location
import io.javalin.websocket.WsContext
import rif.catalog.Catalog

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
 * Sends to viewers from one thread, in order. A new viewer gets its snapshot through the same queue as the live
 * messages, and joins the broadcast right after it, so it sees every message after the snapshot exactly once.
 */
final class Broadcaster extends Runnable:
  private enum Job:
    case All(frame: String)
    case Join(ctx: WsContext, frame: String)
  private val jobs    = LinkedBlockingQueue[Job]()
  private val viewers = ConcurrentHashMap.newKeySet[WsContext]()

  /** Every frame is a JSON array of messages. */
  def publish(msgs: List[ujson.Obj]): Unit = jobs.put(Job.All(ujson.write(ujson.Arr.from(msgs))))
  def join(ctx: WsContext)(snapshot: ujson.Obj): Unit = jobs.put(Job.Join(ctx, ujson.write(ujson.Arr(snapshot))))
  def leave(ctx: WsContext): Unit = viewers.remove(ctx)
  def count: Int = viewers.size

  def run(): Unit =
    try
      while true do
        jobs.take() match
          case Job.All(frame) => viewers.asScala.foreach(send(_, frame))
          case Job.Join(ctx, frame) => if send(ctx, frame) then viewers.add(ctx)
    catch case _: InterruptedException => ()

  private def send(ctx: WsContext, frame: String): Boolean =
    val ok = Try(ctx.session.isOpen && { ctx.send(frame); true }).getOrElse(false)
    if !ok then viewers.remove(ctx)
    ok

/**
 * Prices every event in the flow, tracks money at risk, and streams it to the view, which it also serves.
 *
 * {{{
 * GET  /                         the view
 * WS   /ws                       a snapshot, then frames of event, dead, recovered and stats messages
 * GET  /api/sim/state            the simulator's state
 * POST /api/sim/{action}         break | restore | patch | unpatch | redrive | rate?perSecond=N, passed to the simulator
 * }}}
 */
object Main:
  def main(args: Array[String]): Unit =
    val bootstrap = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
    val port      = sys.env.get("PORT").map(_.toInt).getOrElse(8088)
    val simulator = sys.env.getOrElse("SIMULATOR_URL", "http://localhost:8090")
    val catalog   = Catalog.default

    val latest      = AtomicReference(Option.empty[Polled])
    val snapshots   = LinkedBlockingQueue[ujson.Obj => Unit]()
    val broadcaster = Broadcaster()
    val poller      = Poller(bootstrap, catalog, simulator, latest)
    val reader      = Reader(bootstrap, catalog, broadcaster.publish, snapshots, latest)
    val threads     = List(Thread(broadcaster, "broadcaster"), Thread(poller, "poller"), Thread(reader, "reader"))
    threads.foreach { t => t.setDaemon(true); t.start() }

    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    def toSimulator(method: String, path: String): (Int, String) =
      Try {
        val req = HttpRequest.newBuilder(URI.create(s"$simulator$path")).timeout(Duration.ofSeconds(5))
          .method(method, HttpRequest.BodyPublishers.noBody()).build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        (res.statusCode, res.body)
      }.getOrElse((502, ujson.write(ujson.Obj("error" -> s"the simulator is not reachable at $simulator"))))

    val app = Javalin.create { config =>
      config.staticFiles.add("/web", Location.CLASSPATH)
      config.showJavalinBanner = false
    }
    app.ws("/ws", ws => {
      ws.onConnect { ctx =>
        ctx.enableAutomaticPings()
        snapshots.put(broadcaster.join(ctx))
      }
      ws.onClose(ctx => broadcaster.leave(ctx))
      ws.onError(ctx => broadcaster.leave(ctx))
    })
    app.get("/api/sim/state", ctx => {
      val (code, body) = toSimulator("GET", "/state")
      ctx.status(code).contentType("application/json").result(body)
    })
    app.post("/api/sim/{action}", ctx => {
      val query        = Option(ctx.queryString()).fold("")(q => s"?$q")
      val (code, body) = toSimulator("POST", s"/${ctx.pathParam("action")}$query")
      ctx.status(code).contentType("application/json").result(body)
    })
    app.start(port)
    println(s"observer: Kafka at $bootstrap, simulator at $simulator, view on http://localhost:$port")

    sys.addShutdownHook {
      reader.stop()
      poller.stop()
      app.stop()
    }
